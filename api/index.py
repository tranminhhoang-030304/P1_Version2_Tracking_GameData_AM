from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import time
import json
import os
import requests
import glob
import csv
import io
import re
from collections import defaultdict

# ==========================================
# 1. CẤU HÌNH DATABASE
# ==========================================
# Lấy biến môi trường DATABASE_URL (Do Render cung cấp)
DATABASE_URL = os.getenv("DATABASE_URL")

# Nếu không có (tức là đang chạy local), dùng SQLite
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./game_data.db"
else:
    # Fix lỗi nhỏ của Render: Nó trả về 'postgres://' nhưng SQLAlchemy cần 'postgresql://'
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Cấu hình engine
if "sqlite" in DATABASE_URL:
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    # Cấu hình cho PostgreSQL
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class AppConfig(Base):
    __tablename__ = "app_config"
    id = Column(Integer, primary_key=True, index=True)
    app_name = Column(String, default="My Game")
    appmetrica_app_id = Column(String, default="")
    appmetrica_token = Column(String, default="")
    daily_schedule_time = Column(String, default="09:00")
    interval_minutes = Column(Integer, default=60)

class BoosterItem(Base):
    __tablename__ = "boosters"
    id = Column(Integer, primary_key=True, index=True)
    app_id = Column(Integer, default=1)
    event_param_key = Column(String) # VD: packID_NoAds2
    display_name = Column(String)    # VD: NoAds2
    price = Column(Float, default=0.0)
    source_type = Column(String, default="MANUAL") # Cột Mới: MANUAL / AUTO

class EtlHistory(Base):
    __tablename__ = "execution_history"
    id = Column(Integer, primary_key=True, index=True)
    job_code = Column(String)
    job_name = Column(String)
    start_time = Column(DateTime, default=datetime.now)
    end_time = Column(DateTime, nullable=True)
    status = Column(String)
    rows_processed = Column(Integer, default=0)
    message = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

app = FastAPI()
scheduler = BackgroundScheduler()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Dependency lấy DB (Đã tách dòng chuẩn) ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Hàm Parse tiền tệ (Dùng chung)
def parse_currency(value_str):
    try:
        if isinstance(value_str, (int, float)): return float(value_str)
        clean_str = re.sub(r'[^\d.,]', '', str(value_str))
        clean_str = clean_str.replace(',', '.')
        if clean_str.count('.') > 1:
            parts = clean_str.split('.')
            clean_str = "".join(parts[:-1]) + '.' + parts[-1]
        return float(clean_str)
    except: return 0.0

# ==========================================
# 2. LOGIC TỰ ĐỘNG QUÉT ITEM (AUTO-DISCOVERY)
# ==========================================
def scan_items_from_file(file_path):
    print(f"🔍 Đang quét Items từ file: {file_path}")
    db = SessionLocal()
    new_items_count = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        raw_rows = data.get("raw_response", {}).get("data", [])
        
        # Cache danh sách item hiện có để tránh trùng
        existing_keys = {item.event_param_key for item in db.query(BoosterItem).all()}
        items_to_add = {} 

        for row in raw_rows:
            try:
                dims = row.get("dimensions", [])
                if not dims: continue
                param_str = dims[0].get("name", "")
                if not param_str or param_str == "{}": continue
                param_json = json.loads(param_str)

                # 1. QUÉT IAP PACKS (packID)
                if "packID" in param_json:
                    pack_name = str(param_json["packID"])
                    key = f"packID_{pack_name}"
                    
                    if key not in existing_keys:
                        price = 0.0
                        if "amount" in param_json: price = parse_currency(param_json["amount"])
                        elif "value" in param_json: price = parse_currency(param_json["value"])
                        
                        items_to_add[key] = {
                            "key": key,
                            "name": pack_name,
                            "price": price,
                            "type": "AUTO"
                        }

                # 2. QUÉT BOOSTERS (Keys bắt đầu bằng booster_)
                for k in param_json.keys():
                    if k.startswith("booster_"):
                        booster_name = k.replace("booster_", "")
                        if k not in existing_keys:
                            items_to_add[k] = {
                                "key": k,
                                "name": booster_name,
                                "price": 0.0, 
                                "type": "AUTO"
                            }

            except: continue

        # Lưu vào DB
        for item_data in items_to_add.values():
            db_item = BoosterItem(
                event_param_key=item_data["key"],
                display_name=item_data["name"],
                price=item_data["price"],
                source_type="AUTO"
            )
            db.add(db_item)
            new_items_count += 1
            existing_keys.add(item_data["key"]) 
        
        db.commit()
        if new_items_count > 0:
            print(f"✨ Đã tự động thêm {new_items_count} items mới vào Settings!")
        else:
            print("👌 Không tìm thấy item mới nào.")

    except Exception as e:
        print(f"⚠️ Lỗi khi quét items: {e}")
    finally:
        db.close()

# ==========================================
# 3. LOGIC CÀO DATA (LOGS API + RETRY + AUTO SCAN)
# ==========================================
def run_crawler_logic(job_type="MANUAL"):
    print(f"🚀 [{job_type}] Bắt đầu cào Raw Data từ Logs API...")
    db = SessionLocal()
    job = EtlHistory(job_code=job_type, job_name="AppMetrica Logs Export", status="running", start_time=datetime.now())
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        config = db.query(AppConfig).first()
        if not config or not config.appmetrica_token or not config.appmetrica_app_id:
            raise Exception("Thiếu App ID hoặc Token!")

        url = "https://api.appmetrica.yandex.com/logs/v1/export/events.csv"
        date_since = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        date_until = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        params = {"application_id": config.appmetrica_app_id, "date_since": date_since, "date_until": date_until, "fields": "event_name,event_json,event_timestamp"}
        headers = {"Authorization": f"OAuth {config.appmetrica_token}"}

        max_retries = 10
        response = None
        for attempt in range(max_retries):
            print(f"📡 Kết nối lần {attempt + 1}...")
            response = requests.get(url, params=params, headers=headers, stream=True, timeout=120)
            if response.status_code == 200:
                print("✅ Kết nối thành công!")
                break
            elif response.status_code == 202:
                print("⏳ Server bận (202). Đợi 10s...")
                time.sleep(10)
            else:
                raise Exception(f"Lỗi API ({response.status_code}): {response.text}")
        
        if response.status_code != 200: raise Exception("Timeout.")

        response.encoding = 'utf-8'
        csv_reader = csv.DictReader(io.StringIO(response.text))
        
        raw_rows = []
        for row in csv_reader:
            raw_rows.append({
                "dimensions": [
                    {"name": row.get("event_json", "{}")},
                    {"name": row.get("event_name", "")}
                ],
                "metrics": [1]
            })

        rows_count = len(raw_rows)
        timestamp = int(datetime.now().timestamp())
        filename = f"data_{job_type}_{timestamp}.json"
        os.makedirs("raw_data", exist_ok=True)
        filepath = os.path.join("raw_data", filename)

        with open(filepath, "w", encoding="utf-8") as f:
            final_data = {
                "source": "AppMetrica_Logs_API",
                "crawled_at": str(datetime.now()),
                "job_type": job_type,
                "rows_count": rows_count,
                "raw_response": { "data": raw_rows }
            }
            json.dump(final_data, f, indent=2, ensure_ascii=False)

        job.status = "success"
        job.rows_processed = rows_count
        job.end_time = datetime.now()
        job.message = f"Downloaded {rows_count} events"
        db.commit()
        print(f"✅ [{job_type}] Xong! File: {filepath}")

        # --- GỌI HÀM QUÉT ITEM ---
        scan_items_from_file(filepath)

    except Exception as e:
        print(f"❌ [{job_type}] Lỗi: {e}")
        job.status = "failed"
        job.message = str(e)[0:200]
        db.commit()
    finally:
        db.close()

# ==========================================
# 4. LOGIC DASHBOARD (ĐÃ FIX LEVELID + REVENUE)
# ==========================================
@app.get("/dashboard/{id}")
def get_dashboard_data(id: int):
    list_of_files = glob.glob('raw_data/*.json') 
    if not list_of_files: 
        return {"summary": {"total_revenue": 0}, "chart_data": []}
    
    latest_file = max(list_of_files, key=os.path.getctime)
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            file_content = json.load(f)
            
        raw_rows = file_content.get("raw_response", {}).get("data", [])
        level_stats = defaultdict(lambda: {"revenue": 0, "wins": 0, "fails": 0, "ads": 0, "iap": 0})
        
        for row in raw_rows:
            try:
                dims = row.get("dimensions", [])
                if not dims: continue
                param_str = dims[0].get("name", "")
                event_name = str(dims[1].get("name", "")).strip()
                if not param_str or param_str == "{}": continue
                try: param_json = json.loads(param_str)
                except: continue

                lvl = 0
                if "levelID" in param_json: lvl = int(param_json["levelID"])
                elif "level" in param_json: lvl = int(param_json["level"])
                elif "missionID" in param_json: lvl = int(param_json["missionID"])
                if lvl == 0: continue 

                rev = 0
                if "amount" in param_json: rev = parse_currency(param_json["amount"])
                elif "value" in param_json: rev = parse_currency(param_json["value"])

                if "Complete" in event_name or "Win" in event_name or "Success" in event_name: 
                    level_stats[lvl]["wins"] += 1
                elif "Fail" in event_name or "Lose" in event_name or "Die" in event_name: 
                    level_stats[lvl]["fails"] += 1

                lower_event = event_name.lower()
                src = str(param_json.get("source", "")).lower()
                
                if "ad" in lower_event or "reward" in lower_event or "ad" in src: 
                    level_stats[lvl]["ads"] += 1
                elif "iap" in lower_event or "offer" in lower_event or "purchase" in lower_event:
                     level_stats[lvl]["iap"] += 1
                     level_stats[lvl]["revenue"] += rev
            except: continue

        chart_data = []
        summary = {"total_revenue": 0, "total_ads": 0, "total_iap": 0, "total_wins": 0, "total_fails": 0, "active_players": 0}
        max_lvl = max(level_stats.keys()) if level_stats else 120
        limit_lvl = max(120, max_lvl)

        for i in range(1, limit_lvl + 1):
            stat = level_stats.get(i, {"revenue": 0, "wins": 0, "fails": 0, "ads": 0, "iap": 0})
            total_plays = stat["wins"] + stat["fails"]
            fail_rate = (stat["fails"] / total_plays * 100) if total_plays > 0 else 0
            chart_data.append({"name": f"Lv {i}", "total": stat["revenue"], "failRate": int(fail_rate), "ads": stat["ads"], "iap": stat["iap"]})
            summary["total_revenue"] += stat["revenue"]
            summary["total_ads"] += stat["ads"]
            summary["total_iap"] += stat["iap"]
            summary["total_wins"] += stat["wins"]
            summary["total_fails"] += stat["fails"]

        summary["active_players"] = int(file_content.get("rows_count", 0))
        if chart_data: summary["avg_fail_rate"] = round(sum(x["failRate"] for x in chart_data) / len(chart_data), 1)
        return { "summary": summary, "chart_data": chart_data }

    except Exception as e: return {"summary": {"total_revenue": 0}, "chart_data": []}

# ==========================================
# 5. API KHÁC
# ==========================================
def update_scheduler_jobs(daily_time: str, interval_min: int):
    scheduler.remove_all_jobs()
    try:
        if daily_time and ":" in daily_time:
            h, m = map(int, daily_time.split(":"))
            scheduler.add_job(run_crawler_logic, CronTrigger(hour=h, minute=m), args=["DAILY_JOB"], id="job_daily", replace_existing=True)
    except: pass
    if interval_min > 0:
        scheduler.add_job(run_crawler_logic, IntervalTrigger(minutes=interval_min), args=["INTERVAL_JOB"], id="job_interval", replace_existing=True)

@app.get("/etl/history")
def get_history(db: Session = Depends(get_db)):
    return db.query(EtlHistory).order_by(EtlHistory.id.desc()).limit(50).all()

@app.post("/etl/run/{id}")
def manual_run(id: int): 
    run_crawler_logic("MANUAL_RUN")
    return {"status": "success", "message": "Triggered"}

@app.delete("/etl/history/all")
def delete_history(db: Session = Depends(get_db)):
    db.query(EtlHistory).delete()
    db.commit()
    return {"status": "deleted"}

@app.get("/apps/")
def get_config(db: Session = Depends(get_db)):
    cfg = db.query(AppConfig).first()
    if not cfg: 
        cfg = AppConfig()
        db.add(cfg)
        db.commit()
    return cfg

class ConfigSchema(BaseModel):
    app_name: str
    appmetrica_app_id: str
    appmetrica_token: str
    daily_schedule_time: str
    interval_minutes: int

@app.post("/apps/")
def save_config(config: ConfigSchema, db: Session = Depends(get_db)):
    cfg = db.query(AppConfig).first()
    if not cfg: 
        cfg = AppConfig()
        db.add(cfg)
    cfg.app_name = config.app_name
    cfg.appmetrica_app_id = config.appmetrica_app_id
    cfg.appmetrica_token = config.appmetrica_token
    cfg.daily_schedule_time = config.daily_schedule_time
    cfg.interval_minutes = config.interval_minutes
    db.commit()
    update_scheduler_jobs(cfg.daily_schedule_time, cfg.interval_minutes)
    return {"status": "saved"}

class BoosterSchema(BaseModel):
    app_id: int
    event_param_key: str
    display_name: str
    price: float

@app.get("/boosters/")
def get_boosters(db: Session = Depends(get_db)): 
    return db.query(BoosterItem).all()

@app.post("/boosters/")
def add_booster(item: BoosterSchema, db: Session = Depends(get_db)):
    new_item = BoosterItem(
        app_id=item.app_id,
        event_param_key=item.event_param_key,
        display_name=item.display_name,
        price=item.price,
        source_type="MANUAL"
    )
    db.add(new_item)
    db.commit()
    return {"status": "added"}

@app.delete("/boosters/{id}")
def delete_booster(id: int, db: Session = Depends(get_db)):
    db.query(BoosterItem).filter(BoosterItem.id == id).delete()
    db.commit()
    return {"status": "deleted"}

@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    cfg = db.query(AppConfig).first()
    d_time = cfg.daily_schedule_time if cfg else "09:00"
    i_min = cfg.interval_minutes if cfg else 60
    db.close()
    
    if not scheduler.running: scheduler.start()
    update_scheduler_jobs(d_time, i_min)
    print("✅ SYSTEM READY: Real Data + Auto-Scan Activated.")

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()