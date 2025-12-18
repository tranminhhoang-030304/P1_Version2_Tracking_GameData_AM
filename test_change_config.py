import sqlite3

def change_config():
    db_file = "game_data.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Giả lập sếp đổi cấu hình thành 2 phút/lần
    new_interval = "2" 
    
    print(f"🔄 Đang thay đổi cấu hình trong {db_file}...")
    
    cursor.execute("UPDATE system_config SET value = ? WHERE key = 'AUTO_SYNC_INTERVAL'", (new_interval,))
    conn.commit()
    
    print(f"✅ Đã cập nhật xong! Chu kỳ mới là: {new_interval} phút.")
    print("👉 Hãy quay lại cửa sổ Terminal đang chạy Scheduler để kiểm tra!")

    conn.close()

if __name__ == "__main__":
    change_config()