from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func, case
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from datetime import datetime
import random
import os

# --- SỬA LỖI 1: Dùng thư mục /tmp để chạy được trên Vercel ---
# Trên Vercel, chỉ thư mục /tmp là được phép ghi file
DB_FILE = "/tmp/game_data.db"
DATABASE_URL = f"sqlite:///{DB_FILE}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELS ---
class JobLog(Base):
    __tablename__ = "job_logs"
    id = Column(Integer, primary_key=True, index=True)
    job_name = Column(String, default="ETL Process")
    status = Column(String) 
    rows_imported = Column(Integer)
    message = Column(String)
    start_time = Column(DateTime, default=datetime.now)

class BoosterConfig(Base):
    __tablename__ = "booster_config"
    id = Column(Integer, primary_key=True, index=True)
    booster_key = Column(String, unique=True)
    booster_name = Column(String)
    coin_cost = Column(Integer)

class LevelSessionFact(Base):
    __tablename__ = "fact_level_sessions"
    session_id = Column(String, primary_key=True)
    level_id = Column(Integer, index=True)
    status = Column(String)
    total_coin_spent = Column(Integer)
    event_timestamp = Column(DateTime)

# Tạo bảng (Sẽ tạo lại mỗi khi Vercel khởi động vì nằm trong /tmp)
Base.metadata.create_all(bind=engine)

app = FastAPI()

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

# --- HÀM TẠO DATA GIẢ (Chạy mỗi lần khởi động) ---
def seed_data_if_empty():
    db = SessionLocal()
    # Luôn tạo lại dữ liệu vì /tmp sẽ bị xóa sau một thời gian
    if db.query(LevelSessionFact).count() == 0:
        print("🌱 Đang tạo lại dữ liệu giả trên Vercel...")
        
        # 1. Tạo Booster
        boosters = [
            BoosterConfig(booster_key="hammer", booster_name="Búa Thần", coin_cost=100),
            BoosterConfig(booster_key="bomb", booster_name="Bom Nổ", coin_cost=150),
            BoosterConfig(booster_key="magnet", booster_name="Nam Châm", coin_cost=80),
            BoosterConfig(booster_key="time", booster_name="Thêm Giờ", coin_cost=120),
        ]
        for b in boosters:
            existing = db.query(BoosterConfig).filter_by(booster_key=b.booster_key).first()
            if not existing:
                db.add(b)
        
        # 2. Tạo Logs
        db.add(JobLog(status="SUCCESS", rows_imported=500, message="Vercel Deploy Success", start_time=datetime.now()))
        
        # 3. Tạo 200 lượt chơi
        for i in range(200):
            lvl = random.randint(1, 10) # Level 1-10
            is_fail = random.choice([True, False, False])
            status = "FAIL" if is_fail else "SUCCESS"
            db.add(LevelSessionFact(
                session_id=f"sess_{i}_{random.randint(1000,9999)}",
                level_id=lvl,
                status=status,
                total_coin_spent=random.randint(0, 500),
                event_timestamp=datetime.now()
            ))
        db.commit()
    db.close()

seed_data_if_empty()

# --- API ENDPOINTS (Đã sửa tên cho khớp Frontend) ---

@app.get("/api/health")
def health_check():
    return {"status": "ok", "env": "Vercel"}

# SỬA LỖI 2: Đổi tên endpoint từ /level-stats thành /items-by-level
@app.get("/api/analytics/items-by-level")
def get_items_by_level(db: Session = Depends(get_db)):
    results = db.query(
        LevelSessionFact.level_id,
        func.sum(LevelSessionFact.total_coin_spent).label("revenue"),
        func.sum(case((LevelSessionFact.status == 'FAIL', 1), else_=0)).label("total_fail"),
        func.count(LevelSessionFact.session_id).label("total_play")
    ).group_by(LevelSessionFact.level_id).all()
    
    data = []
    for r in results:
        fail_rate = 0
        if r.total_play and r.total_play > 0:
            fail_rate = round(((r.total_fail or 0) / r.total_play) * 100, 1)
        
        data.append({
            "level": f"Level {r.level_id}", 
            "revenue": r.revenue or 0, 
            "fail_rate": fail_rate
        })
    # Sort theo level
    data.sort(key=lambda x: int(x['level'].split()[1]))
    return data

@app.get("/api/analytics/booster-stats")
def get_booster_stats(db: Session = Depends(get_db)):
    boosters = db.query(BoosterConfig).all()
    data = []
    for b in boosters:
        data.append({
            "name": b.booster_name, 
            "used": random.randint(10, 200)
        })
    data.sort(key=lambda x: x['used'], reverse=True)
    return data

@app.get("/api/monitor/logs")
def get_logs(db: Session = Depends(get_db)):
    return db.query(JobLog).order_by(JobLog.start_time.desc()).limit(10).all()

@app.get("/api/settings/boosters")
def get_settings_boosters(db: Session = Depends(get_db)):
    return db.query(BoosterConfig).all()