# api/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

# Ưu tiên lấy POSTGRES_URL, nếu không có thì dùng SQLite local để test
SQLALCHEMY_DATABASE_URL = os.getenv("POSTGRES_URL", "sqlite:///./game_data.db")

# Nếu dùng SQLite thì cần tham số check_same_thread, Postgres thì không cần
connect_args = {"check_same_thread": False} if "sqlite" in SQLALCHEMY_DATABASE_URL else {}

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()