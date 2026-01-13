import psycopg2

# --- CẤU HÌNH DATABASE (SỬA LẠI MẬT KHẨU CỦA BẠN) ---
DB_HOST = "localhost"
DB_NAME = "game_analytics_db"
DB_USER = "postgres"
DB_PASS = "tranminhhoang-030304" # <--- SỬA MẬT KHẨU Ở ĐÂY

def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS public.apps (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            app_id VARCHAR(255) NOT NULL UNIQUE,
            api_token VARCHAR(255),
            client_id VARCHAR(255),
            is_active BOOLEAN DEFAULT TRUE,
            schedule_time VARCHAR(50) DEFAULT '00:00',
            interval_minutes INTEGER DEFAULT 60,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.job_history (
            id SERIAL PRIMARY KEY,
            app_id INTEGER REFERENCES public.apps(id) ON DELETE CASCADE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status VARCHAR(50),
            total_events INTEGER DEFAULT 0,
            run_type VARCHAR(50),
            logs TEXT,
            success_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.event_logs (
            id SERIAL PRIMARY KEY,
            app_id INTEGER REFERENCES public.apps(id) ON DELETE CASCADE,
            event_name VARCHAR(255),
            event_json TEXT,
            count INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    
    conn = None
    try:
        print("dang ket noi database...")
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        
        print("dang tao bang (create tables)...")
        for command in commands:
            cur.execute(command)
            
        cur.close()
        conn.commit()
        print("✅ THANH CONG! DATABASE DA DUOC KHOI TAO.")
    except Exception as e:
        print("❌ LOI ROI BAN OI:", e)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    create_tables()