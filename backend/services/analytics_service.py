from sqlalchemy.orm import Session
from sqlalchemy import func, case
from datetime import date
from typing import List
from backend.models import Item
from backend.models.transaction import Transaction
from backend.schemas.analytics import RevenueData, FailRateData, ItemDistribution, ItemByLevel


class AnalyticsService:
    def __init__(self, db: Session):
        self.db = db

    def get_revenue_data(self, start_date: date, end_date: date) -> List[RevenueData]:
        """Get revenue data aggregated by date"""
        try:
            results = self.db.query(
                func.date(Transaction.transaction_date).label('date'),
                func.sum(Transaction.amount).label('revenue'),
                func.count(Transaction.id).label('transactions')
            ).filter(
                Transaction.status == 'success',
                func.date(Transaction.transaction_date) >= start_date,
                func.date(Transaction.transaction_date) <= end_date
            ).group_by(
                func.date(Transaction.transaction_date)
            ).order_by(
                func.date(Transaction.transaction_date)
            ).all()

            return [
                RevenueData(
                    date=r.date,
                    revenue=float(r.revenue or 0),
                    transactions=int(r.transactions or 0)
                ) for r in results
            ]
        except Exception:
            return []

    def get_fail_rate_data(self, start_date: date, end_date: date) -> List[FailRateData]:
        """Get fail rate statistics aggregated by date"""
        try:
            results = self.db.query(
                func.date(Transaction.transaction_date).label('date'),
                func.count(Transaction.id).label('total_attempts'),
                func.sum(case((Transaction.status == 'failed', 1), else_=0)).label('failed_attempts')
            ).filter(
                func.date(Transaction.transaction_date) >= start_date,
                func.date(Transaction.transaction_date) <= end_date
            ).group_by(
                func.date(Transaction.transaction_date)
            ).order_by(
                func.date(Transaction.transaction_date)
            ).all()

            return [
                FailRateData(
                    date=r.date,
                    total_attempts=int(r.total_attempts or 0),
                    failed_attempts=int(r.failed_attempts or 0),
                    fail_rate=(float(r.failed_attempts or 0) / float(r.total_attempts or 1) * 100) if r.total_attempts > 0 else 0
                ) for r in results
            ]
        except Exception:
            return []

    def get_items_by_level(self) -> List[ItemDistribution]:
        """Get item distribution grouped by level"""
        try:
            results = self.db.query(
                Item.level,
                func.count(Transaction.id).label('count'),
                func.sum(Transaction.amount).label('total_revenue')
            ).join(
                Transaction, Item.id == Transaction.item_id
            ).filter(
                Transaction.status == 'success'
            ).group_by(
                Item.level
            ).order_by(
                Item.level
            ).all()

            return [
                ItemDistribution(
                    level=r.level,
                    count=int(r.count or 0),
                    total_revenue=float(r.total_revenue or 0)
                ) for r in results
            ]
        except Exception:
            return []

    def get_items_detail_by_level(self, level: int) -> List[ItemByLevel]:
        """Get detailed item statistics for a specific level"""
        try:
            results = self.db.query(
                Item.id.label('item_id'),
                Item.name.label('item_name'),
                func.count(Transaction.id).label('count'),
                func.sum(Transaction.amount).label('revenue')
            ).join(
                Transaction, Item.id == Transaction.item_id
            ).filter(
                Item.level == level,
                Transaction.status == 'success'
            ).group_by(
                Item.id, Item.name
            ).order_by(
                func.count(Transaction.id).desc()
            ).all()

            return [
                ItemByLevel(
                    item_id=r.item_id,
                    item_name=r.item_name,
                    count=int(r.count or 0),
                    revenue=float(r.revenue or 0)
                ) for r in results
            ]
        except Exception:
            return []

    def get_booster_stats(self):
        """Get top boosters/items by usage count"""
        try:
            results = self.db.query(
                Item.name.label('booster_name'),
                func.count(Transaction.id).label('usage_count'),
                func.sum(Transaction.amount).label('total_revenue')
            ).join(
                Transaction, Item.id == Transaction.item_id
            ).filter(
                Transaction.status == 'success'
            ).group_by(
                Item.name
            ).order_by(
                func.count(Transaction.id).desc()
            ).limit(10).all()

            return [
                {
                    'booster_name': r.booster_name,
                    'usage_count': int(r.usage_count or 0),
                    'total_revenue': float(r.total_revenue or 0)
                } for r in results
            ]
        except Exception:
            return []

    def get_level_booster_breakdown(self, level: int):
        """Get booster breakdown for a specific level"""
        try:
            results = self.db.query(
                Item.name.label('booster_name'),
                func.count(Transaction.id).label('count'),
                func.sum(Transaction.amount).label('revenue')
            ).join(
                Transaction, Item.id == Transaction.item_id
            ).filter(
                Item.level == level,
                Transaction.status == 'success'
            ).group_by(
                Item.name
            ).order_by(
                func.count(Transaction.id).desc()
            ).all()

            return [
                {
                    'booster_name': r.booster_name,
                    'count': int(r.count or 0),
                    'revenue': float(r.revenue or 0)
                } for r in results
            ]
        except Exception:
            return []
        except Exception:
            return []