import sqlite3
import os
from datetime import datetime

# Tên file database hiện tại của bạn
DB_NAME = "game_data.db"

def create_tables():
    # Kiểm tra xem file DB có tồn tại không
    if not os.path.exists(DB_NAME):
        print(f"❌ Lỗi: Không tìm thấy file {DB_NAME}. Hãy chắc chắn bạn đang chạy lệnh ở thư mục gốc dự án.")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print(f"🔌 Đang kết nối tới {DB_NAME}...")

    # 1. Tạo bảng Cấu hình hệ thống (System Config)
    # Lưu các setting như: Tự động chạy (ON/OFF), Chu kỳ chạy (30 phút, 60 phút...)
    print("🛠  Đang tạo bảng 'system_config'...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS system_config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        description TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Thêm dữ liệu mặc định ban đầu (Nếu chưa có)
    configs = [
        ('AUTO_SYNC_ENABLED', 'false', 'Bật/Tắt chế độ tự động cào dữ liệu'),
        ('AUTO_SYNC_INTERVAL', '60', 'Khoảng thời gian giữa các lần cào (phút)'),
        ('LAST_SUCCESSFUL_SYNC', '', 'Thời điểm cuối cùng cào dữ liệu thành công')
    ]
    
    for config in configs:
        cursor.execute('''
        INSERT OR IGNORE INTO system_config (key, value, description)
        VALUES (?, ?, ?)
        ''', config)

    # 2. Tạo bảng Lịch sử thực thi (Execution History)
    # Lưu lại nhật ký mỗi lần cào: Thành công hay thất bại, lưu file nào, bao nhiêu dòng...
    print("🛠  Đang tạo bảng 'execution_history'...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS execution_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        status TEXT NOT NULL, -- 'SUCCESS', 'FAILED', 'SKIPPED'
        records_fetched INTEGER DEFAULT 0,
        file_path TEXT,       -- Đường dẫn file JSON thô đã lưu
        message TEXT,         -- Thông báo chi tiết (hoặc lỗi)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    conn.commit()
    conn.close()
    print("✅ Cập nhật Database thành công! Các bảng mới đã sẵn sàng.")

if __name__ == "__main__":
    create_tables()