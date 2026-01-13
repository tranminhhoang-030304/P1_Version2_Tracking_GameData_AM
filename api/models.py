# api/models.py
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Float, Text
from datetime import datetime
from .database import Base # Lưu ý dấu chấm .database

class App(Base):
    __tablename__ = "apps"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    app_id = Column(String, unique=True)
    api_token = Column(String)
    client_id = Column(String, nullable=True)
    is_active = Column(Boolean, default=False)
    is_mock = Column(Boolean, default=False)

class JobHistory(Base):
    __tablename__ = "job_history"
    id = Column(Integer, primary_key=True, index=True)
    app_id = Column(Integer, ForeignKey("apps.id"))
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    status = Column(String) 
    total_events = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_log = Column(Text, nullable=True)

class EventLog(Base):
    __tablename__ = "event_logs"
    id = Column(Integer, primary_key=True, index=True)
    app_id = Column(Integer, ForeignKey("apps.id"))
    event_name = Column(String, index=True)
    event_type = Column(String) # Booster / Normal
    count = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)

class AnalyticsConfig(db.Model):
    __tablename__ = 'analytics_config'

    id = Column(Integer, primary_key=True, index=True)
    app_id = Column(Integer, ForeignKey('apps.id'), unique=True, nullable=False)
    
    # Mapping Events
    level_start_event = Column(String(100), default='')
    level_win_event = Column(String(100), default='')
    level_fail_event = Column(String(100), default='')

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Quan hệ ngược về App (Optional)
    # app = db.relationship("App", back_populates="analytics_config")

    def to_dict(self):
        return {
            "level_start": self.level_start_event,
            "level_win": self.level_win_event,
            "level_fail": self.level_fail_event
        }

class BoosterConfig(base.Model):
    __tablename__ = 'booster_configs'

    id = Column(Integer, primary_key=True, index=True)
    app_id = Column(Integer, ForeignKey('apps.id'), nullable=False)
    
    event_name = Column(String(100), nullable=False)
    display_name = Column(String(100))
    coin_cost = Column(Integer, default=0)

    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "event_name": self.event_name,
            "display_name": self.display_name,
            "coin_cost": self.coin_cost
        }

class LevelAnalytics(Base):
    __tablename__ = 'level_analytics'

    id = Column(Integer, primary_key=True)
    app_id = Column(Integer, ForeignKey('apps.id'), nullable=False)
    
    # Định danh
    session_id = Column(String(100), index=True) 
    user_id = Column(String(100), index=True)
    level_name = Column(String(100))
    
    # Kết quả
    status = Column(String(20)) # WIN / FAIL
    duration = Column(Integer, default=0)
    start_time = Column(DateTime)
    
    # [QUAN TRỌNG] Cột JSONB
    boosters_used = Column(JSONB, default={}) 
    
    total_cost = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "level_name": self.level_name,
            "status": self.status,
            "duration": self.duration,
            "boosters_used": self.boosters_used,
            "total_cost": self.total_cost,
            "start_time": self.start_time
        }