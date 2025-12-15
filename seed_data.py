"""
Seed script to populate the database with mock data:
- Items across 20 levels
- A number of transactions for analytics testing
- 3600 ETL log entries

Usage:
    POSTGRES_URL=... python seed_data.py
"""
import os
import random
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import Base
from backend.models import Item
from backend.models.transaction import Transaction
from backend.models.etl_log import EtlLog


def main():
    db_url = os.environ.get("POSTGRES_URL") or getattr(settings, "POSTGRES_URL", None)
    if not db_url:
        raise RuntimeError("POSTGRES_URL environment variable is required to run seed_data.py")

    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as session:
        # Create items: 20 levels, 10 items per level => 200 items
        items = []
        for level in range(1, 21):
            for i in range(1, 11):
                items.append(Item(name=f"Item L{level}-{i}", level=level, price=round(random.uniform(0.99, 99.99), 2)))

        session.bulk_save_objects(items)
        session.commit()

        # Refresh items list with ids
        all_items = session.query(Item).all()
        item_ids = [it.id for it in all_items]

        # Create transactions: e.g., 2000 random transactions over last 90 days
        transactions = []
        now = datetime.utcnow()
        for _ in range(2000):
            item_id = random.choice(item_ids)
            amount = round(random.uniform(0.99, 199.99), 2)
            status = "success" if random.random() > 0.05 else "failed"
            tx_time = now - timedelta(days=random.randint(0, 89), seconds=random.randint(0, 86400))
            transactions.append(Transaction(player_id=random.randint(1, 500), item_id=item_id, amount=amount, status=status, transaction_date=tx_time))

        session.bulk_save_objects(transactions)
        session.commit()

        # Create 3600 ETL log entries (one per hour backwards)
        etl_logs = []
        for i in range(3600):
            run_time = now - timedelta(hours=i)
            status = "success" if random.random() > 0.03 else "failed"
            records = random.randint(0, 1000) if status == "success" else 0
            duration = round(random.uniform(0.1, 10.0), 3)
            etl_logs.append(EtlLog(run_time=run_time, status=status, records_processed=records, duration_seconds=duration))

        # Bulk insert in batches for safety
        batch_size = 1000
        for i in range(0, len(etl_logs), batch_size):
            session.bulk_save_objects(etl_logs[i:i+batch_size])
            session.commit()

        print("Seeding complete: items, transactions, etl_logs")


if __name__ == "__main__":
    main()

