from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey, String
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from backend.database import Base


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, nullable=False, index=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(String(20), nullable=False, index=True)
    fail_reason = Column(String(255), nullable=True)
    transaction_date = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    item = relationship("Item", backref="transactions")

    def __repr__(self):
        return f"<Transaction(id={self.id}, status='{self.status}')>"
