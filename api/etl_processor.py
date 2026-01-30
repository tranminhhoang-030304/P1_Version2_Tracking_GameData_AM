import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Copy cấu hình DB từ main.py sang (hoặc import từ config nếu có)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def get_db_connection():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        return conn
    except Exception as e:
        print(f"❌ ETL DB Connection Error: {e}")
        return None

def run_etl_pipeline(app_id):
    """
    Hàm này sẽ:
    1. Đọc cấu hình Game (Start/Win/Fail event)
    2. Đọc bảng giá Booster
    3. Quét Log thô -> Gom nhóm thành Level Session
    4. Tính toán tiền & item -> Ghi vào bảng level_analytics
    """
    print(f"🚀 [ETL] Bắt đầu xử lý dữ liệu cho App ID: {app_id}")
    conn = get_db_connection()
    if not conn: return False
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Lấy Config (Luật chơi)
        cur.execute("SELECT * FROM analytics_config WHERE app_id = %s", (app_id,))
        config = cur.fetchone()
        if not config:
            print("⚠️ [ETL] Chưa có cấu hình Start/Win/Fail. Vui lòng config trên Web trước.")
            return False
            
        evt_start = config['level_start_event']
        evt_win = config['level_win_event']
        evt_fail = config['level_fail_event']

        # 2. Lấy Bảng giá Booster (để tính tiền)
        cur.execute("SELECT booster_event_name, cost FROM booster_configs WHERE app_id = %s", (app_id,))
        boosters = cur.fetchall()
        # Biến đổi thành Dict để tra cứu cho nhanh: {'buy_hammer': 500, ...}
        price_map = {b['booster_event_name']: b['cost'] for b in boosters}
        
        # 3. Lấy tất cả sự kiện START (Trong thực tế nên lọc những cái chưa xử lý)
        # Sắp xếp theo User và Thời gian để dễ xử lý
        print("⏳ [ETL] Đang quét log Start...")
        cur.execute("""
            SELECT id, event_json, created_at 
            FROM event_logs 
            WHERE app_id = %s AND event_name = %s 
            ORDER BY created_at ASC
        """, (app_id, evt_start))
        start_logs = cur.fetchall()
        
        processed_count = 0
        
        for start_row in start_logs:
            # Parse JSON lấy UserID, LevelID
            try:
                raw_json = start_row['event_json']
                params = {}

                # 1. Xử lý (JSON lồng nhau) để lấy dữ liệu
                if isinstance(raw_json, dict):
                    # Nếu là Dict, kiểm tra xem có key 'event_json' dạng string bên trong không
                    if 'event_json' in raw_json and isinstance(raw_json['event_json'], str):
                         params = json.loads(raw_json['event_json'])
                    else:
                         params = raw_json
                else:
                    # Nếu là String, parse ra Dict rồi lại kiểm tra lồng nhau
                    temp = json.loads(raw_json)
                    if 'event_json' in temp and isinstance(temp['event_json'], str):
                        params = json.loads(temp['event_json'])
                    else:
                        params = temp

                # 2. Map dữ liệu (Sửa để khớp với log của bạn)
                # Tạo User ID giả: GUEST + ID log (Vì log thiếu user_id)
                user_id = params.get('uid') or params.get('user_id') or f"Guest_{start_row['id']}"

                # Lấy Level: Log của bạn dùng key 'dayChallenge'
                level_val = params.get('dayChallenge') or params.get('level_id') or params.get('level')
                # Đặt tên biến là level_id để khớp với câu lệnh INSERT ở dưới cùng
                level_id = f"Day_{level_val}" if level_val else "Unknown_Level"

                # Tạo Session ID duy nhất
                session_id = f"{user_id}_{start_row['id']}"
                
            except Exception as e:
                print(f"⚠️ Lỗi parse JSON dòng {start_row['id']}: {e}")
                continue

            # 4. Tìm sự kiện KẾT THÚC (Win/Fail) gần nhất của user này
            cur.execute("""
                SELECT event_name, created_at 
                FROM event_logs 
                WHERE app_id = %s 
                  AND event_name IN (%s, %s)
                  AND created_at > %s 
                  AND (event_json::jsonb->>'user_id' = %s OR event_json::jsonb->>'user' = %s)
                ORDER BY created_at ASC 
                LIMIT 1
            """, (app_id, evt_win, evt_fail, start_row['created_at'], str(user_id), str(user_id)))
            
            end_row = cur.fetchone()
            
            # Mặc định (nếu không thấy kết thúc -> User thoát ngang xương)
            status = 'DROP'
            duration = 0
            end_time = start_row['created_at'] + timedelta(minutes=10) # Giới hạn tìm booster 10p
            
            if end_row:
                status = 'WIN' if end_row['event_name'] == evt_win else 'FAIL'
                end_time = end_row['created_at']
                # Tính duration (giây)
                diff = end_time - start_row['created_at']
                duration = int(diff.total_seconds())

            # 5. Quét Booster đã mua trong khoảng thời gian chơi (Start -> End)
            # Chỉ tìm những event có trong bảng giá (price_map)
            if price_map:
                placeholders = ', '.join(['%s'] * len(price_map))
                query_booster = f"""
                    SELECT event_name 
                    FROM event_logs 
                    WHERE app_id = %s 
                      AND event_name IN ({placeholders})
                      AND created_at >= %s AND created_at <= %s
                      AND (event_json::jsonb->>'user_id' = %s OR event_json::jsonb->>'user' = %s)
                """
                params = [app_id] + list(price_map.keys()) + [start_row['created_at'], end_time, str(user_id), str(user_id)]
                
                cur.execute(query_booster, tuple(params))
                booster_logs = cur.fetchall()
            else:
                booster_logs = []

            # Tổng hợp tiền & số lượng booster
            current_boosters = {}
            total_cost = 0
            
            for b in booster_logs:
                b_name = b['event_name']
                current_boosters[b_name] = current_boosters.get(b_name, 0) + 1
                total_cost += price_map.get(b_name, 0) # Cộng dồn tiền

            # 6. GHI VÀO DB (Upsert - Nếu trùng session_id thì update)
            # Lưu boosters_used dưới dạng chuỗi JSON
            boosters_json = json.dumps(current_boosters)
            
            cur.execute("""
                INSERT INTO level_analytics 
                (app_id, session_id, user_id, level_name, status, duration, start_time, boosters_used, total_cost, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING 
            """, (app_id, session_id, str(user_id), str(level_id), status, duration, start_row['created_at'], boosters_json, total_cost))
            
            # Lưu ý: Ở trên mình dùng "ON CONFLICT (id) DO NOTHING" vì id tự tăng. 
            # Để tránh duplicate dữ liệu chính xác, ta nên check trước:
            
            processed_count += 1
            
        conn.commit()
        print(f"✅ [ETL] Hoàn tất! Đã tổng hợp {processed_count} lượt chơi.")
        return True
        
    except Exception as e:
        print(f"❌ [ETL] Lỗi xử lý: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()