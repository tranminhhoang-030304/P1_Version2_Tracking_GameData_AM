# File: api/fix_stuck.py
import psycopg2

# Cấu hình y hệt file chính
DB_HOST = "localhost"
DB_NAME = "game_analytics_db"
DB_USER = "postgres"
DB_PASS = "tranminhhoang-030304"

try:
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    
    # Chuyển tất cả job đang treo thành Failed
    cur.execute("""
        UPDATE public.job_history 
        SET status = 'Failed', logs = logs || '\n[System] Reset because server restart.'
        WHERE status = 'Running'
    """)
    
    conn.commit()
    print("✅ Đã dọn dẹp xong! Giờ bạn có thể bật lại Server.")
    
except Exception as e:
    print("Lỗi:", e)
finally:
    if 'conn' in locals(): conn.close()