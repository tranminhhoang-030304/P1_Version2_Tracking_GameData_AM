import sqlite3

def check_data():
    conn = sqlite3.connect("game_data.db")
    cursor = conn.cursor()

    print("\n=== 1. KIỂM TRA BẢNG CẤU HÌNH (SYSTEM_CONFIG) ===")
    cursor.execute("SELECT key, value FROM system_config WHERE key='LAST_SUCCESSFUL_SYNC'")
    row = cursor.fetchone()
    if row:
        print(f"✅ Thời gian cào cuối cùng (LAST_SUCCESSFUL_SYNC): {row[1]}")
    else:
        print("❌ Chưa thấy cấu hình thời gian!")

    print("\n=== 2. KIỂM TRA LỊCH SỬ (EXECUTION_HISTORY) ===")
    # Lấy 5 dòng mới nhất
    cursor.execute("SELECT id, start_time, status, message, file_path FROM execution_history ORDER BY id DESC LIMIT 5")
    rows = cursor.fetchall()
    
    if rows:
        print(f"✅ Tìm thấy {len(rows)} dòng log:")
        for r in rows:
            print(f"   [ID: {r[0]}] Time: {r[1]} | Status: {r[2]} | Msg: {r[3]}")
            if r[4]: print(f"   -> File: {r[4]}")
    else:
        print("❌ Chưa có lịch sử nào được ghi!")

    conn.close()

if __name__ == "__main__":
    check_data()