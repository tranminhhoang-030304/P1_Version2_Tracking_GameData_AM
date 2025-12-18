import time
import sqlite3
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from crawler import run_crawler_job  # Import hàm cào dữ liệu ta đã viết

# Cấu hình log để dễ theo dõi
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

DB_NAME = "game_data.db"
SCHEDULER_JOB_ID = "appmetrica_sync_job"

def get_config_interval():
    """Đọc cấu hình chu kỳ chạy (phút) từ Database"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM system_config WHERE key = 'AUTO_SYNC_INTERVAL'")
        row = cursor.fetchone()
        conn.close()
        if row:
            return int(row[0])
    except Exception as e:
        logger.error(f"Lỗi đọc config: {e}")
    return 60 # Mặc định 60 phút nếu lỗi hoặc chưa cấu hình

def check_and_reschedule(scheduler):
    """Hàm này chạy định kỳ (mỗi 1 phút) để kiểm tra xem Sếp có đổi lịch không"""
    current_job = scheduler.get_job(SCHEDULER_JOB_ID)
    new_interval = get_config_interval()

    # Nếu chưa có job nào, tạo mới
    if not current_job:
        logger.info(f"Khởi tạo Job với chu kỳ {new_interval} phút.")
        scheduler.add_job(
            run_crawler_job, 
            'interval', 
            minutes=new_interval, 
            id=SCHEDULER_JOB_ID,
            replace_existing=True
        )
        return

    # Nếu có job rồi, kiểm tra xem chu kỳ hiện tại có khớp với Database không
    # (Trigger của IntervalTrigger lưu interval dưới dạng timedelta)
    current_interval_minutes = int(current_job.trigger.interval.total_seconds() / 60)

    if current_interval_minutes != new_interval:
        logger.info(f"⚠️ Phát hiện thay đổi cấu hình! Đổi từ {current_interval_minutes} phút -> {new_interval} phút.")
        scheduler.reschedule_job(
            SCHEDULER_JOB_ID, 
            trigger=IntervalTrigger(minutes=new_interval)
        )
        # Chạy ngay lập tức 1 lần cho sếp vui
        logger.info("Chạy ngay lập tức để áp dụng cấu hình mới...")
        scheduler.modify_job(SCHEDULER_JOB_ID, next_run_time=None) 

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.start()
    logger.info("⏳ Hệ thống lập lịch đã khởi động (Scheduler Started).")

    # 1. Đăng ký job cào dữ liệu lần đầu tiên
    initial_interval = get_config_interval()
    scheduler.add_job(
        run_crawler_job, 
        'interval', 
        minutes=initial_interval, 
        id=SCHEDULER_JOB_ID
    )
    logger.info(f"Đã lên lịch cào dữ liệu: {initial_interval} phút/lần.")

    # 2. Đăng ký một job phụ: Cứ 1 phút kiểm tra Database 1 lần xem sếp có đổi giờ không
    # (Đây là kỹ thuật Dynamic Rescheduling)
    scheduler.add_job(
        check_and_reschedule,
        'interval',
        minutes=1, # Check config mỗi 1 phút
        args=[scheduler]
    )

    try:
        # Giữ cho chương trình chạy mãi mãi (để scheduler sống)
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Hệ thống lập lịch đã tắt.")

if __name__ == "__main__":
    start_scheduler()