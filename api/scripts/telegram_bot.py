import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import json
import os
from dotenv import load_dotenv # [NEW] Nhập thư viện đọc file .env

# [NEW] Ép kịch bản đọc file .env ở thư mục gốc
load_dotenv() 

# ================= CẤU HÌNH BOT =================
TELEGRAM_TOKEN = "8665230769:AAHDchXDaK9YiBROTSzYFbaDjpakEgGpjd0"
CHAT_ID = "-5179798117"
APP_ID = 2 # ID game Loopy Bus Escape của bạn

# ================= CẤU HÌNH DATABASE =================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),  # Bắt buộc đổi chữ "database" thành "dbname"
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT", "5432") # Thêm port cho chắc cú
}

def send_telegram_message(message):
    """Hàm bắn tin nhắn lên Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML" # Dùng HTML để in đậm, in nghiêng cho đẹp
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"Lỗi gửi Telegram: {response.text}")
    except Exception as e:
        print(f"Lỗi kết nối Telegram: {e}")

def run_daily_report():
    """Hàm móc Data từ Database và tổng hợp báo cáo"""
    # Lấy ngày hôm qua
    yesterday = datetime.now() - timedelta(days=1)
    start_date = yesterday.strftime('%Y-%m-%d 00:00:00')
    end_date = yesterday.strftime('%Y-%m-%d 23:59:59')
    display_date = yesterday.strftime('%d/%m/%Y')

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. ĐẾM TỔNG SỐ EVENT HÔM QUA
        cur.execute("""
            SELECT COUNT(*) as total_events 
            FROM event_logs 
            WHERE app_id = %s 
              AND (created_at + interval '7 hours') >= %s 
              AND (created_at + interval '7 hours') <= %s
        """, (APP_ID, start_date, end_date))
        total_events = cur.fetchone()['total_events']

        # 2. TÌM KẺ MẤT TÍCH Ở LEVEL 0
        cur.execute("""
            WITH Level0_Logs AS (
                SELECT event_name, (event_json::json)->>'uuid' as uid
                FROM event_logs
                WHERE app_id = %s
                  AND event_name IN ('level_start', 'level_first_start', 'level_win', 'level_first_end')
                  AND (event_json::json)->>'level_display' = '0'
                  AND (created_at + interval '7 hours') >= %s
                  AND (created_at + interval '7 hours') <= %s
            ),
            StartUsers AS (SELECT DISTINCT uid FROM Level0_Logs WHERE event_name LIKE '%%start%%'),
            WinUsers AS (SELECT DISTINCT uid FROM Level0_Logs WHERE event_name LIKE '%%win%%' OR event_name LIKE '%%end%%')
            
            SELECT uid FROM StartUsers
            EXCEPT
            SELECT uid FROM WinUsers;
        """, (APP_ID, start_date, end_date))
        
        dropped_rows = cur.fetchall()
        dropped_uuids = [row['uid'] for row in dropped_rows if row['uid']]
        dropped_count = len(dropped_uuids)

        # ================= SOẠN TIN NHẮN (BỆNH ÁN) =================
        msg = f"🏥 <b>BÁO CÁO GAME DAILY (Ngày {display_date})</b>\n"
        msg += f"-----------------------------------\n"
        msg += f"📊 <b>Tổng Event thu thập:</b> {total_events:,}\n\n"
        
        if dropped_count > 0:
            msg += f"🚨 <b>BÁO ĐỘNG RỚT LEVEL 0:</b> Có <b>{dropped_count}</b> User bị kẹt!\n"
            msg += f"<i>🔍 Danh sách UUID (Giới hạn 5 người đầu):</i>\n"
            for uid in dropped_uuids[:5]:
                msg += f"<code>{uid}</code>\n" # Thẻ <code> giúp click vào là tự copy trên đt
            msg += f"\n👉 Các sếp Dev @all vui lòng check lại luồng chơi!"
        else:
            msg += f"✅ <b>LEVEL 0 XANH MƯỚT:</b> Không có User nào rớt giữa đường. Tuyệt vời! 🎉"

        # Bắn tin nhắn!
        send_telegram_message(msg)
        print("Đã gửi báo cáo thành công!")

    except Exception as e:
        error_msg = f"❌ <b>LỖI HỆ THỐNG AUTO-BOT:</b>\nKhông thể tạo báo cáo. Lỗi: {str(e)}"
        send_telegram_message(error_msg)
        print(f"Lỗi: {e}")
    finally:
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    run_daily_report()