from pydantic import BaseModel
from datetime import date


class RevenueData(BaseModel):
    date: date
    revenue: float
    transactions: int


class FailRateData(BaseModel):
    date: date
    fail_rate: float
    total_attempts: int
    failed_attempts: int


class ItemDistribution(BaseModel):
    level: int
    count: int
    total_revenue: float


class ItemByLevel(BaseModel):
    item_id: int
    item_name: str
    count: int
    revenue: float
