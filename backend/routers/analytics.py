from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List
from datetime import date, datetime, timedelta
from backend.database import get_db
from backend.schemas.analytics import RevenueData, FailRateData, ItemDistribution, ItemByLevel
from backend.services.analytics_service import AnalyticsService

router = APIRouter()

@router.get("/revenue", response_model=List[RevenueData])
async def get_revenue(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    db: Session = Depends(get_db)
):
    if not start_date:
        start_date = datetime.now().date() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now().date()

    service = AnalyticsService(db)
    return service.get_revenue_data(start_date, end_date)

@router.get("/fail-rate", response_model=List[FailRateData])
async def get_fail_rate(
    start_date: date = Query(default=None),
    end_date: date = Query(default=None),
    db: Session = Depends(get_db)
):
    if not start_date:
        start_date = datetime.now().date() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now().date()

    service = AnalyticsService(db)
    return service.get_fail_rate_data(start_date, end_date)

@router.get("/items-by-level", response_model=List[ItemDistribution])
async def get_items_by_level(db: Session = Depends(get_db)):
    service = AnalyticsService(db)
    return service.get_items_by_level()

@router.get("/items-by-level/{level}", response_model=List[ItemByLevel])
async def get_items_detail_by_level(level: int, db: Session = Depends(get_db)):
    service = AnalyticsService(db)
    return service.get_items_detail_by_level(level)

@router.get("/booster-stats")
async def get_booster_stats(db: Session = Depends(get_db)):
    """Get top boosters/items by usage count"""
    service = AnalyticsService(db)
    return service.get_booster_stats()

@router.get("/level-booster-breakdown/{level}")
async def get_level_booster_breakdown(level: int, db: Session = Depends(get_db)):
    """Get booster breakdown for a specific level"""
    service = AnalyticsService(db)
    return service.get_level_booster_breakdown(level)
