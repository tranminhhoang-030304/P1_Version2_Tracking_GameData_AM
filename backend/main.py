from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import time
import random
import json
import os

# ==========================================
# 1. CẤU HÌNH DATABASE
# ==========================================
DATABASE_URL = "sqlite:///./game_data.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Model 1: Cấu hình App (Lưu 2 chế độ chạy) ---
class AppConfig(Base):
    __tablename__ = "app_config"
    id = Column(Integer, primary_key=True, index=True)
    app_name = Column(String, default="My Game")
    appmetrica_app_id = Column(String, default="")
    appmetrica_token = Column(String, default="")
    
    # Hai cột cấu hình riêng biệt cho Scheduler
    daily_schedule_time = Column(String, default="09:00") # Giờ cố định (VD: "09:00")
    interval_minutes = Column(Integer, default=60)        # Chu kỳ (VD: 60 phút)

# --- Model 2: Vật phẩm (Settings) ---
class BoosterItem(Base):
    __tablename__ = "boosters"
    id = Column(Integer, primary_key=True, index=True)
    app_id = Column(Integer, default=1)
    event_param_key = Column(String)
    display_name = Column(String)
    price = Column(Float)

# --- Model 3: Lịch sử ETL (Monitor) ---
class EtlHistory(Base):
    __tablename__ = "execution_history"
    id = Column(Integer, primary_key=True, index=True)
    job_code = Column(String) # "DAILY_JOB", "INTERVAL_JOB", "MANUAL"
    job_name = Column(String)
    start_time = Column(DateTime, default=datetime.now)
    end_time = Column(DateTime, nullable=True)
    status = Column(String) # success / failed / running
    rows_processed = Column(Integer, default=0)
    message = Column(String, nullable=True)

# Tạo bảng
Base.metadata.create_all(bind=engine)

# ==========================================
# 2. KHỞI TẠO APP & SCHEDULER
# ==========================================
app = FastAPI()
scheduler = BackgroundScheduler()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==========================================
# 3. LOGIC CÀO DỮ LIỆU (CORE)
# ==========================================
def run_crawler_logic(job_type="MANUAL"):
    print(f"🚀 [{job_type}] Đang chạy cào dữ liệu...")
    db = SessionLocal()
    
    # 1. Ghi log Running
    job = EtlHistory(
        job_code=job_type, 
        job_name=f"Data Import ({job_type})", 
        status="running", 
        start_time=datetime.now()
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        # 2. Giả lập quá trình cào data (Delay 2s)
        time.sleep(2)
        
        # Tạo dữ liệu giả & lưu file
        rows = random.randint(500, 5000)
        timestamp = int(datetime.now().timestamp())
        filename = f"data_{job_type}_{timestamp}.json"
        
        # Đảm bảo thư mục tồn tại
        os.makedirs("raw_data", exist_ok=True)
        filepath = os.path.join("raw_data", filename)

        with open(filepath, "w", encoding="utf-8") as f:
            data = {
                "source": job_type,
                "timestamp": str(datetime.now()),
                "rows_count": rows,
                "data": [{"id": i, "event": "level_finish"} for i in range(10)] # Sample
            }
            json.dump(data, f, indent=2)

        # 3. Cập nhật Success
        job.status = "success"
        job.rows_processed = rows
        job.end_time = datetime.now()
        job.message = f"Saved: {filename}"
        db.commit()
        print(f"✅ [{job_type}] Thành công! File: {filepath}")

    except Exception as e:
        print(f"❌ [{job_type}] Lỗi: {e}")
        job.status = "failed"
        job.message = str(e)
        db.commit()
    finally:
        db.close()

# ==========================================
# 4. HÀM CẬP NHẬT LỊCH (HYBRID SCHEDULE)
# ==========================================
def update_scheduler_jobs(daily_time: str, interval_min: int):
    """
    Hàm này sẽ xóa sạch lịch cũ và đặt lại 2 lịch mới song song
    """
    scheduler.remove_all_jobs()
    log_msg = []

    # 1. Setup Job Hàng Ngày (Cron Trigger)
    try:
        if daily_time and ":" in daily_time:
            h, m = map(int, daily_time.split(":"))
            scheduler.add_job(
                run_crawler_logic, 
                CronTrigger(hour=h, minute=m), 
                args=["DAILY_JOB"], 
                id="job_daily",
                replace_existing=True
            )
            log_msg.append(f"Daily at {h}:{m:02d}")
    except Exception as e:
        print(f"⚠️ Lỗi định dạng giờ: {e}")

    # 2. Setup Job Chu Kỳ (Interval Trigger)
    if interval_min > 0:
        scheduler.add_job(
            run_crawler_logic, 
            IntervalTrigger(minutes=interval_min), 
            args=["INTERVAL_JOB"], 
            id="job_interval",
            replace_existing=True
        )
        log_msg.append(f"Every {interval_min} mins")

    print(f"⏰ [Scheduler Updated] Chế độ chạy: {' + '.join(log_msg) if log_msg else 'None'}")

# ==========================================
# 5. CÁC API ENDPOINTS
# ==========================================

# --- A. API DASHBOARD (120 LEVELS) ---
@app.get("/dashboard/{id}")
def get_dashboard_data(id: int):
    # Tạo dữ liệu giả cho 120 Level
    chart_data = []
    for i in range(1, 121): # Từ level 1 đến 120
        # Logic giả lập: Level cao -> Doanh thu giảm, Fail tăng
        base_rev = 50000 - (i * 350) 
        if base_rev < 1000: base_rev = 1000 + random.randint(0, 500)
        
        fail_rate = 10 + (i * 0.6)
        if fail_rate > 90: fail_rate = 90 + random.randint(-5, 5)

        chart_data.append({
            "name": f"Lv {i}",
            "total": int(base_rev),
            "failRate": int(fail_rate),
            "ads": int(base_rev * 0.3),
            "iap": int(base_rev * 0.7)
        })

    return {
        "summary": {
            "total_revenue": sum(x["total"] for x in chart_data),
            "avg_fail_rate": round(sum(x["failRate"] for x in chart_data) / len(chart_data), 1),
            "active_players": random.randint(10000, 50000),
            "total_ads": sum(x["ads"] for x in chart_data),
            "total_iap": sum(x["iap"] for x in chart_data),
            "total_wins": random.randint(5000, 15000),
            "total_fails": random.randint(2000, 8000)
        },
        "chart_data": chart_data
    }

# --- B. API MONITOR ---
@app.get("/etl/history")
def get_history(db: Session = Depends(get_db)):
    return db.query(EtlHistory).order_by(EtlHistory.id.desc()).limit(50).all()

@app.post("/etl/run/{id}")
def manual_run(id: int):
    # Chạy ngay lập tức (Không chờ lịch)
    run_crawler_logic("MANUAL_RUN")
    return {"status": "success", "message": "Manual job triggered"}

@app.delete("/etl/history/all")
def delete_history(db: Session = Depends(get_db)):
    db.query(EtlHistory).delete()
    db.commit()
    return {"status": "deleted"}

# --- C. API SETTINGS (CẤU HÌNH SONG SONG) ---
class ConfigSchema(BaseModel):
    app_name: str
    appmetrica_app_id: str
    appmetrica_token: str
    daily_schedule_time: str # Input 1: "09:00"
    interval_minutes: int    # Input 2: 60

@app.get("/apps/")
def get_config(db: Session = Depends(get_db)):
    cfg = db.query(AppConfig).first()
    if not cfg:
        # Tạo mặc định
        cfg = AppConfig(daily_schedule_time="09:00", interval_minutes=60)
        db.add(cfg)
        db.commit()
    return cfg

@app.post("/apps/")
def save_config(config: ConfigSchema, db: Session = Depends(get_db)):
    # 1. Lưu vào DB
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

    # 2. Cập nhật Scheduler ngay lập tức
    update_scheduler_jobs(cfg.daily_schedule_time, cfg.interval_minutes)
    
    return {"status": "saved", "message": "Config saved & Scheduler updated"}

# --- D. API ITEMS ---
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
    new_item = BoosterItem(**item.dict())
    db.add(new_item)
    db.commit()
    return {"status": "added"}

@app.delete("/boosters/{id}")
def delete_booster(id: int, db: Session = Depends(get_db)):
    db.query(BoosterItem).filter(BoosterItem.id == id).delete()
    db.commit()
    return {"status": "deleted"}

# ==========================================
# 6. STARTUP EVENT
# ==========================================
@app.on_event("startup")
def startup_event():
    # Load cấu hình từ DB lên để chạy Scheduler
    db = SessionLocal()
    cfg = db.query(AppConfig).first()
    d_time = cfg.daily_schedule_time if cfg else "09:00"
    i_min = cfg.interval_minutes if cfg else 60
    db.close()
    
    if not scheduler.running:
        scheduler.start()
    
    update_scheduler_jobs(d_time, i_min)
    print("✅ System Started Ready.")

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()