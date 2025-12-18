import sqlite3
import json
import os
import requests
from datetime import datetime, timedelta
import time

# --- CẤU HÌNH ---
DB_NAME = "game_data.db"
RAW_DATA_FOLDER = "raw_data"
# API AppMetrica (Bạn điền thông tin thật vào .env hoặc sửa trực tiếp ở đây để test)
APPMETRICA_API_URL = "https://api.appmetrica.yandex.com/stat/v1/data"
APPMETRICA_TOKEN = os.getenv("APPMETRICA_TOKEN", "YOUR_OAUTH_TOKEN") 
APPMETRICA_APP_ID = os.getenv("APPMETRICA_APP_ID", "YOUR_APP_ID")

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_last_sync_time():
    """Lấy mốc thời gian cuối cùng cào thành công từ DB"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Lấy config
    cursor.execute("SELECT value FROM system_config WHERE key = 'LAST_SUCCESSFUL_SYNC'")
    row = cursor.fetchone()
    conn.close()

    if row and row['value']:
        return datetime.fromisoformat(row['value'])
    
    # Mặc định: Nếu chưa chạy bao giờ, lấy dữ liệu từ 2 ngày trước (hoặc tùy bạn chỉnh)
    return datetime.now() - timedelta(days=2)

def update_last_sync_time(sync_time):
    """Cập nhật mốc thời gian mới vào DB"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE system_config SET value = ? WHERE key = 'LAST_SUCCESSFUL_SYNC'", (sync_time.isoformat(),))
    conn.commit()
    conn.close()

def log_execution(start_time, status, message, records=0, file_path=None):
    """Ghi log vào bảng execution_history để hiển thị lên Monitor"""
    conn = get_db_connection()
    cursor = conn.cursor()
    end_time = datetime.now()
    
    cursor.execute('''
        INSERT INTO execution_history (start_time, end_time, status, records_fetched, file_path, message)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (start_time, end_time, status, records, file_path, message))
    
    conn.commit()
    conn.close()
    print(f"[{status}] {message}")

def fetch_from_appmetrica(date_from, date_to):
    """
    Hàm gọi API AppMetrica.
    LƯU Ý: Đây là cấu trúc chuẩn. Nếu bạn chưa có Token thật, 
    nó sẽ trả về lỗi hoặc dữ liệu mẫu.
    """
    # Format thời gian theo yêu cầu AppMetrica (YYYY-MM-DD HH:mm:ss)
    fmt = "%Y-%m-%d %H:%M:%S"
    str_from = date_from.strftime(fmt)
    str_to = date_to.strftime(fmt)
    
    print(f"📡 Đang request từ: {str_from} đến {str_to}...")

    # --- BLOCK GỌI API THẬT (Bỏ comment khi có Token xịn) ---
    # params = {
    #     'id': APPMETRICA_APP_ID,
    #     'date1': str_from, # Lưu ý: AppMetrica params có thể khác tùy endpoint
    #     'date2': str_to,
    #     'metrics': 'ym:ge:users', # Ví dụ metric
    #     'dimensions': 'ym:ge:date',
    #     'limit': 10000
    # }
    # headers = {'Authorization': f'OAuth {APPMETRICA_TOKEN}'}
    # try:
    #     response = requests.get(APPMETRICA_API_URL, params=params, headers=headers)
    #     if response.status_code == 200:
    #         return response.json().get('data', []) # Giả sử data nằm trong key 'data'
    #     else:
    #         print(f"API Error: {response.text}")
    #         return None
    # except Exception as e:
    #     print(f"Exception: {e}")
    #     return None
    # --------------------------------------------------------

    # --- DỮ LIỆU GIẢ LẬP (MOCK DATA) ĐỂ TEST LOGIC ---
    # (Xóa phần này khi chạy thật)
    mock_data = [
        {"event": "level_up", "user_id": 123, "timestamp": str_to},
        {"event": "purchase", "user_id": 456, "timestamp": str_to}
    ]
    return mock_data

def run_crawler_job():
    start_time = datetime.now()
    print("🚀 Bắt đầu tiến trình cào dữ liệu...")

    # 1. Tính toán khung thời gian (Time Window)
    last_sync = get_last_sync_time()
    
    # Quy tắc sếp: Delay 1 tiếng (Current - 1h)
    target_to_time = datetime.now() - timedelta(hours=1)

    # Làm tròn về phút để tránh lệch giây lẻ
    target_to_time = target_to_time.replace(second=0, microsecond=0)
    last_sync = last_sync.replace(second=0, microsecond=0)

    # 2. Kiểm tra điều kiện chạy
    # Nếu khoảng thời gian < 15 phút, bỏ qua để tránh spam file rác
    time_diff = (target_to_time - last_sync).total_seconds() / 60
    if time_diff < 15:
        msg = f"Khoảng thời gian quá ngắn ({time_diff} phút). Chờ thêm dữ liệu mới."
        log_execution(start_time, "SKIPPED", msg)
        return

    # 3. Gọi API lấy dữ liệu
    data = fetch_from_appmetrica(last_sync, target_to_time)

    if data is None:
        log_execution(start_time, "FAILED", "Lỗi khi gọi API AppMetrica.")
        return

    if len(data) == 0:
        # Cập nhật mốc thời gian dù không có data, để lần sau không phải quét lại đoạn này
        update_last_sync_time(target_to_time)
        log_execution(start_time, "SUCCESS", "Không có dữ liệu mới trong khoảng thời gian này.", 0)
        return

    # 4. Lưu file JSON (Raw Data)
    # Tạo cấu trúc thư mục theo ngày: raw_data/2023-10-27/
    date_folder = target_to_time.strftime("%Y-%m-%d")
    save_dir = os.path.join(RAW_DATA_FOLDER, date_folder)
    ensure_directory_exists(save_dir)

    # Tên file: data_10-00_to_11-00.json
    file_name = f"data_{last_sync.strftime('%H-%M')}_to_{target_to_time.strftime('%H-%M')}.json"
    full_path = os.path.join(save_dir, file_name)

    try:
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # 5. Cập nhật thành công
        update_last_sync_time(target_to_time)
        log_execution(start_time, "SUCCESS", f"Đã lưu {len(data)} bản ghi mới.", len(data), full_path)
    
    except Exception as e:
        log_execution(start_time, "FAILED", f"Lỗi khi ghi file: {str(e)}")

if __name__ == "__main__":
    # Chạy thử 1 lần khi gọi trực tiếp file này
    run_crawler_job()