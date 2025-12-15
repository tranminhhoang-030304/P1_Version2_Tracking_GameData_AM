from sqlalchemy import Column, Integer, String, DateTime, Float, Text
from sqlalchemy.sql import func
from backend.database import Base


class EtlLog(Base):
    __tablename__ = "etl_logs"

    id = Column(Integer, primary_key=True, index=True)
    run_time = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    status = Column(String(20), nullable=False, index=True)
    records_processed = Column(Integer, default=0)
    duration_seconds = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    log_metadata = Column(String, nullable=True)

    def __repr__(self):
        return f"<EtlLog(id={self.id}, status='{self.status}')>"
