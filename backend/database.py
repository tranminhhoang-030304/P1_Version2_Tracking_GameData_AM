from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Lưu thẳng vào file game_data.db
SQLALCHEMY_DATABASE_URL = "sqlite:///./game_data.db"

# connect_args quan trọng cho SQLite để không bị lỗi thread
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()