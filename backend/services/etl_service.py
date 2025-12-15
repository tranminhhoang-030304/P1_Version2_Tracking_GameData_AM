import random
from datetime import datetime
from sqlalchemy.orm import Session
from backend.models.etl_log import EtlLog # Đảm bảo tên file model đúng là etl_log.py

class EtlService:
    def run_etl_job(self, db: Session):
        # 1. Xác định kết quả ngẫu nhiên (80% thành công)
        is_success = random.choice([True, True, True, True, False])
        
        # 2. Tạo số liệu logic
        if is_success:
            status = "SUCCESS"
            rows = random.randint(1000, 50000) # Thành công thì phải có dữ liệu
            message = "Data imported successfully."
        else:
            status = "FAILED"
            rows = 0 # Thất bại thì không có dòng nào
            message = "Connection timeout waiting for Oracle DB."

        # 3. Lưu vào Database
        new_log = EtlLog(
            job_name=f"ETL-DAILY-{datetime.now().strftime('%H%M%S')}",
            status=status,
            rows_imported=rows,
            message=message,
            start_time=datetime.now()
        )
        
        db.add(new_log)
        db.commit() # Quan trọng: Phải Commit thì F5 mới không mất
        db.refresh(new_log)
        
        return new_log