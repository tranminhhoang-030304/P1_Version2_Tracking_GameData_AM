from etl_processor import run_etl_pipeline
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta
import time
import requests
import threading
import random
import os                       
from dotenv import load_dotenv
import re

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

load_dotenv()
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# --- 3. SỬA CẤU HÌNH DATABASE (Lấy từ .env) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "p1_gamedata")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS") 

# Kiểm tra an toàn: Nếu không đọc được pass thì báo lỗi
if not DB_PASS:
    print("⚠️  CẢNH BÁO: Chưa tìm thấy DB_PASS trong file .env")

# --- CẤU HÌNH HỆ THỐNG ---
MAX_RETRIES = 18       # Số lần thử lại tối đa cho Auto Worker

# --- TRẠNG THÁI HỆ THỐNG TOÀN CỤC ---
SYSTEM_STATE = {
    "is_busy": False,          
    "current_app_id": None,    
    "current_run_type": None   
}
SYSTEM_LOCK = threading.Lock() 
JOB_STOP_EVENTS = {}

def universal_flatten(raw_input):
    """
    Hàm V95: Khoan sâu vào mọi ngóc ngách của JSON (Hỗ trợ n lớp lồng nhau).
    Trả về: Một dictionary phẳng chứa tất cả thông tin.
    """
    if not raw_input: return {}
    
    data = {}
    # Lớp 1: Parse từ DB (thường là string hoặc dict)
    try:
        if isinstance(raw_input, str):
            data = json.loads(raw_input)
        elif isinstance(raw_input, dict):
            data = raw_input.copy()
    except: return {}

    # Lớp 2: Kiểm tra các key chứa JSON lồng nhau thường gặp
    # Game 2 thường nhét dữ liệu vào key 'event_json' hoặc 'params'
    nested_keys = ['event_json', 'params', 'data', 'attributes']
    
    for key in nested_keys:
        if key in data and isinstance(data[key], str):
            try:
                inner = json.loads(data[key])
                if isinstance(inner, dict):
                    data.update(inner) # Gộp dữ liệu con ra ngoài
            except: pass
            
    # Lớp 3: Xử lý Double Encode (Trường hợp chuỗi bị mã hóa 2 lần)
    if isinstance(data, str): 
        try: data = json.loads(data)
        except: pass
        
    return data if isinstance(data, dict) else {}

def get_app_config(cur, app_id):
    """
    Hàm lấy cấu hình động từ Database.
    Nếu không có, trả về cấu hình mặc định (Fallback) để tránh lỗi.
    """
    try:
        cur.execute("SELECT config_json FROM analytics_config WHERE app_id = %s", (app_id,))
        row = cur.fetchone()
        if row and row['config_json']:
            return row['config_json']
    except Exception as e:
        print(f"Config Warning: {e}")
        # Quan trọng: Nếu query lỗi, phải rollback để không kẹt transaction sau này
        if cur.connection:
            cur.connection.rollback()

    # Cấu hình Mặc định (Fallback) nếu chưa setup DB
    return {
        "events": {
            "start": ["missionStart", "missionStart_Daily", "missionStart_WeeklyQuestTutor"],
            "win": ["missionComplete", "missionComplete_Daily", "missionComplete_WeeklyQuestTutor"],
            "progress": ["missionProgress"],
            "fail": ["missionFail", "missionFail_Daily", "missionFail_WeeklyQuestTutor"],
            "transaction": {
                "real_currency": ["iapSuccess", "firstIAP"], # <--- Đã thêm dấu phẩy
                "virtual_currency_exclude": ["iapSuccess", "firstIAP", "iapPurchase", "priceSpendLevel"], # <--- Đã thêm dấu phẩy
                "offer_and_reward": ["FirstReward", "adsRewardComplete", "iapOfferGet", "dailyReward"]
            }
        },
        "boosters": [ # <--- Sửa ngoặc nhọn { thành ngoặc vuông [
            {"key": "booster_Hammer", "name": "Hammer 🔨", "type": "booster"},
            {"key": "booster_Magnet", "name": "Magnet 🧲", "type": "booster"},
            {"key": "booster_Add", "name": "Add Moves ➕", "type": "booster"},
            {"key": "booster_Unlock", "name": "Unlock 🔓", "type": "booster"},
            {"key": "booster_Clear", "name": "Clear 🧹", "type": "booster"},
            {"key": "revive_boosterClear", "name": "Revive 💖", "type": "revive"}
        ], # <--- Sửa ngoặc nhọn } thành ngoặc vuông [
        "currency": {
            "real": ["VND", "USD", "₫", "$"], # <--- Đã thêm dấu phẩy
            "virtual": ["Coin"]
        }
    }

def smart_parse_json(raw_input):
    """
    Hàm thông minh để xử lý trường hợp JSON bị lồng 2 lớp.
    Ví dụ: "{\"event_json\": \"{\\\"levelID\\\": 1...}\"}"
    """
    if not raw_input: 
        return {}
    
    try:
        # Lớp 1: Nếu là string thì parse ra dict, nếu là dict rồi thì giữ nguyên
        parsed_data = json.loads(raw_input) if isinstance(raw_input, str) else raw_input
        
        # Lớp 2: Kiểm tra xem bên trong có key 'event_json' chứa string JSON nữa không (Lỗi double encode)
        if isinstance(parsed_data, dict) and 'event_json' in parsed_data:
            inner_value = parsed_data['event_json']
            if isinstance(inner_value, str):
                try:
                    inner_json = json.loads(inner_value)
                    # Gộp dữ liệu bên trong ra ngoài (Flatten)
                    parsed_data.update(inner_json)
                except:
                    pass # Nếu không parse được lớp trong thì thôi
                    
        return parsed_data
    except Exception:
        return {}

def get_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        return conn
    except Exception as e:
        print("❌ LỖI KẾT NỐI DB:", e)
        return None

# --- HÀM QUẢN LÝ TRẠNG THÁI BẬN/RẢNH ---
def set_system_busy(busy, app_id=None, run_type=None):
    with SYSTEM_LOCK:
        SYSTEM_STATE["is_busy"] = busy
        SYSTEM_STATE["current_app_id"] = app_id
        SYSTEM_STATE["current_run_type"] = run_type

def is_system_busy():
    with SYSTEM_LOCK:
        return SYSTEM_STATE["is_busy"]

# ==========================================
# PHẦN 1: CORE FUNCTIONS (TẠO JOB & CẬP NHẬT)
# ==========================================

def create_etl_job(app_id, date_since, date_until):
    conn = get_db()
    if not conn: return
    cur = conn.cursor()
    try:
        # Check trùng: Nếu đã có job đang chờ/chạy cùng khung giờ thì thôi
        cur.execute("""
            SELECT id FROM etl_jobs 
            WHERE app_id = %s AND date_since = %s AND status IN ('pending', 'processing')
        """, (app_id, date_since))
        if cur.fetchone(): 
            return # Đã có job rồi, không tạo thêm

        cur.execute("""
            INSERT INTO etl_jobs (app_id, date_since, date_until, status, retry_count, message, created_at)
            VALUES (%s, %s, %s, 'pending', 0, 'Scheduled Auto', NOW())
        """, (app_id, date_since, date_until))
        conn.commit()
        print(f"🎫 Auto: Đã tạo vé Job cho App {app_id}")
    except Exception as e:
        print(f"❌ Auto Error: {e}")
    finally:
        cur.close()
        conn.close()

def update_job_status(job_id, status, message=None, inc_retry=False):
    conn = get_db()
    if not conn: return
    cur = conn.cursor()
    try:
        sql = "UPDATE etl_jobs SET status = %s, updated_at = NOW(), message = %s"
        if inc_retry: sql += ", retry_count = retry_count + 1"
        sql += " WHERE id = %s"
        cur.execute(sql, (status, message, job_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()

# ==========================================
# PHẦN 2: WORKER THÔNG MINH (ĐÃ SỬA LỖI TIME & INSERT TRƯỚC)
# ==========================================
# --- HÀM PHỤ TRỢ: GHI LOG VÀO DB ---
def append_log_to_db(hist_id, new_log_line):
    """Nối thêm log vào dòng lịch sử đang chạy"""   
    if not hist_id: return
    try:
        conn = get_db()
        cur = conn.cursor()
        # Dùng toán tử || để nối chuỗi trong PostgreSQL
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = f"\n[{timestamp}] {new_log_line}"
        
        cur.execute("""
            UPDATE job_history 
            SET logs = COALESCE(logs, '') || %s, updated_at = NOW()
            WHERE id = %s
        """, (log_entry, hist_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Error appending log: {e}")

def transform_events_to_level_analytics(app_id, events):
    """
    [UPDATED] Transform missionStart / missionComplete / missionFail
    Sử dụng smart_parse_json để xử lý lỗi lồng JSON.
    """
    if not events:
        return

    conn = get_db()
    cur = conn.cursor()

    # 1. Gom event theo (user_id, level_id)
    sessions = {}

    for e in events:
        try:
            event_name = e.get("event_name")
            raw_json = smart_parse_json(e.get("event_json"))
            level_id = (raw_json.get("levelID") or 
                        raw_json.get("missionID") or 
                        raw_json.get("level_display") or 
                        raw_json.get("level_display_origin"))
            user_id = (raw_json.get("userID") or "Guest" or
                       raw_json.get("uuid"))
            # Nếu vẫn không lấy được level_id, bỏ qua event này
            if not level_id: continue
            session_key = f"{user_id}_{level_id}"
            # Timestamp xử lý an toàn
            try:
                ts_val = int(e.get("event_timestamp"))
                ts = datetime.fromtimestamp(ts_val)
            except:
                ts = datetime.now()

            if session_key not in sessions:
                sessions[session_key] = {
                    "app_id": app_id,
                    "user_id": user_id,
                    "level_id": level_id,
                    "start_time": None,
                    "end_time": None,
                    "status": "DROP",
                    "total_cost": 0
                }

            s = sessions[session_key]

            # Logic tính toán giữ nguyên, chỉ đảm bảo raw_json đã sạch
            if event_name == "missionStart":
                s["start_time"] = ts

            elif event_name == "missionComplete":
                s["end_time"] = ts
                s["status"] = "WIN"
                for k, v in raw_json.items():
                    if k.startswith("booster_") or k.startswith("revive_"):
                        try: s["total_cost"] += int(v)
                        except: pass

            elif event_name == "missionFail":
                s["end_time"] = ts
                s["status"] = "FAIL"
                for k, v in raw_json.items():
                    if k.startswith("booster_") or k.startswith("revive_"):
                        try: s["total_cost"] += int(v)
                        except: pass

        except Exception as ex:
            print(f"Transform error skipping row: {ex}")

    # 2. Insert vào level_analytics
    for s in sessions.values():
        start_time = s["start_time"]
        end_time = s["end_time"]
        duration = 0
        if start_time and end_time:
            duration = int((end_time - start_time).total_seconds())

        # Chỉ insert nếu có dữ liệu hợp lệ (Tránh rác)
        try:
            cur.execute("""
                INSERT INTO level_analytics
                (app_id, session_id, user_id, level_name, status, duration, start_time, total_cost, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """, (
                s["app_id"],
                f"{s['user_id']}_{s['level_id']}",
                s["user_id"],
                f"Level {s['level_id']}", # Format tên Level đẹp hơn
                s["status"],
                duration,
                start_time,
                s["total_cost"]
            ))
        except Exception as insert_err:
            print(f"Insert Analytics Error: {insert_err}")

    conn.commit()
    conn.close()

def worker_process_jobs():
    if is_system_busy(): return

    conn = get_db()
    if not conn: return
    
    # 1. Lấy Job đang chờ
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM etl_jobs 
        WHERE status IN ('pending', 'processing') 
        ORDER BY created_at ASC LIMIT 1
    """)
    job = cur.fetchone()
    cur.close() 
    conn.close()

    if not job: return

    job_id = job['id']
    app_id = job['app_id']
    retry_count = job['retry_count']

    # ================= [BẮT ĐẦU ĐOẠN CẦN THÊM] =================
    # LOGIC: Nếu thấy là Retry, tự động lội ngược dòng tìm ngày cũ
    # (Ghi đè lại date_since/date_until mà API đã tính sai)
    if job.get('run_type') == 'retry' and job.get('retry_job_id'):
        try:
            print(f"🕵️‍♂️ Worker phát hiện Retry cho Job #{job['retry_job_id']}. Đang tính lại ngày...")
            conn_fix = get_db()
            cur_fix = conn_fix.cursor(cursor_factory=RealDictCursor)
            
            # Lấy giờ chạy của Job quá khứ
            cur_fix.execute("SELECT start_time FROM job_history WHERE id = %s", (job['retry_job_id'],))
            old_job = cur_fix.fetchone()
            cur_fix.close()
            conn_fix.close()

            if old_job and old_job['start_time']:
                # Tính lại cửa sổ thời gian (giống logic chu kỳ 1 tiếng)
                fix_target = old_job['start_time']
                fix_from = fix_target - timedelta(minutes=65)
                
                # CẬP NHẬT LẠI DỮ LIỆU TRONG BỘ NHỚ
                job['date_until'] = fix_target.strftime('%Y-%m-%d %H:%M:%S')
                job['date_since'] = fix_from.strftime('%Y-%m-%d %H:%M:%S')
                print(f"✅ Đã điều chỉnh thời gian về quá khứ: {job['date_since']} -> {job['date_until']}")
        except Exception as e_fix:
            print(f"⚠️ Lỗi khi tính lại ngày Retry: {e_fix}")
    # ================= [KẾT THÚC ĐOẠN CẦN THÊM] =================

    # 2. Xử lý quá hạn Retry
    if retry_count >= MAX_RETRIES:
        print(f"💀 Job #{job_id} MAX RETRIES. Failed.")
        update_job_status(job_id, 'failed', f"Timeout: {retry_count} retries.")
        # Cập nhật history lần cuối nếu có
        conn = get_db()
        cur = conn.cursor()
        cur.execute("UPDATE job_history SET status='Failed', end_time=NOW() WHERE app_id=%s AND status IN ('Running','Processing')", (app_id,))
        conn.commit()
        conn.close()
        return 

    set_system_busy(True, app_id, 'auto')

    # 3. --- [QUAN TRỌNG] TÌM HOẶC TẠO HISTORY ---
    # Mục đích: Để các lần Retry sau vẫn nối vào log của lần đầu tiên
    hist_id = None
    try:
        conn = get_db()
        cur = conn.cursor()
        # Tìm history đang chạy dở (Processing) của App này
        cur.execute("""
            SELECT id FROM job_history 
            WHERE app_id = %s AND status IN ('Running', 'Processing') 
            ORDER BY start_time DESC LIMIT 1
        """, (app_id,))
        row = cur.fetchone()
        
        if row:
            hist_id = row[0] # Dùng lại ID cũ để nối log
        else:
            # Tạo mới nếu chưa có (Lần chạy đầu tiên)
            cur.execute("""
                INSERT INTO job_history (app_id, start_time, status, run_type, logs, total_events)
                VALUES (%s, NOW(), 'Processing', 'schedule', '', 0)
                RETURNING id
            """, (app_id,))
            hist_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ History Init Error: {e}")

    try:
        # Hàm log cục bộ: vừa in ra màn hình, vừa đẩy vào DB ngay lập tức
        def log(msg):
            print(msg)
            append_log_to_db(hist_id, msg)

        log(f"▶️ Worker picking up Job #{job_id} (Retry: {retry_count}/{MAX_RETRIES})")

        try:
            # 1. Parse chuỗi giờ UTC từ Database ra
            utc_start = datetime.strptime(str(job['date_since']), '%Y-%m-%d %H:%M:%S')
            utc_end = datetime.strptime(str(job['date_until']), '%Y-%m-%d %H:%M:%S')
            
            # 2. Cộng thêm 7 tiếng để ra giờ Việt Nam
            vn_start = utc_start + timedelta(hours=7)
            vn_end = utc_end + timedelta(hours=7)
            
            # 3. Format lại cho đẹp (Giống Terminal)
            log(f" 🕒 Scanning Window: VN[{vn_start.strftime('%H:%M')} - {vn_end.strftime('%H:%M')}] (UTC: {utc_start.strftime('%H:%M')} - {utc_end.strftime('%H:%M')})")
        except:
            # Fallback: Nếu lỗi format thì in nguyên gốc
            log(f" 🕒 Scanning Window: {job['date_since']} -> {job['date_until']}")

        # Lấy thông tin App
        conn = get_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM apps WHERE id = %s", (app_id,))
        app_info = cur.fetchone()
        cur.close()
        conn.close()

        if not app_info:
            log("❌ Error: App not found or deleted.")
            update_job_status(job_id, 'failed', 'App deleted')
            return
        
        clean_app_id = str(app_info['app_id']).strip()
        clean_token = str(app_info['api_token']).strip()
        # Gọi API AppMetrica
        url = "https://api.appmetrica.yandex.com/logs/v1/export/events.json"
        params = {
            "application_id": clean_app_id,
            "date_since": str(job['date_since']), 
            "date_until": str(job['date_until']),
            "fields": "event_name,event_timestamp,event_json",
            "limit": 1000000 
        }
        headers = {"Authorization": f"OAuth {clean_token}"}

        log(f"  📡 Connecting to AppMetrica...")
        response = requests.get(url, params=params, headers=headers, stream=True, timeout=600)
        
        if response.status_code == 200:
            log("  ✅ Connection Established (200 OK). Downloading...")
            
            conn = get_db()
            cur = conn.cursor()
            
            data = response.json()
            events = data.get('data', [])
            event_count = len(events)
            
            # Insert dữ liệu
            for event in events:
                evt_name = event.get('event_name', 'unknown')
                evt_json = json.dumps(event)
                try: ts = datetime.fromtimestamp(int(event.get('event_timestamp')))
                except: ts = datetime.now()
                
                cur.execute("""
                    INSERT INTO event_logs (app_id, event_name, event_json, count, created_at) 
                    VALUES (%s, %s, %s, 1, %s)
                """, (app_id, evt_name, evt_json, ts))
            try:
                transform_events_to_level_analytics(app_id, events)
                # LỖI CŨ: logs(...) -> ĐỔI THÀNH log(...)
                log(f"ETL transform completed for {len(events)} events") 
            except Exception as e:
                # LỖI CŨ: logs(...) -> ĐỔI THÀNH log(...)
                log(f"ETL transform error: {str(e)}")       
            # --- CẬP NHẬT TRẠNG THÁI CUỐI CÙNG ---
            # Status: Success, cập nhật End Time -> Duration sẽ tính đúng
            if hist_id:
                cur.execute("""
                    UPDATE job_history 
                    SET end_time = NOW(), status = 'Success', total_events = %s, success_count = %s
                    WHERE id = %s
                """, (event_count, event_count, hist_id))
            
            conn.commit()
            conn.close()
            
            update_job_status(job_id, 'completed', f"Done. {event_count} events.")
            log(f"  🎉 Job Completed. Imported: {event_count} events.")

        elif response.status_code == 202:
            wait_time=180
            log(f"  ⏳ HTTP 202: Data not ready. Waiting...")
            # Không đóng History, để trạng thái Processing để lần sau nối log tiếp
            update_job_status(job_id, 'processing', 'Waiting for AppMetrica (202)...', inc_retry=True)
            time.sleep(wait_time) 
        
        elif response.status_code == 429:
            # 1. In toàn bộ nội dung lỗi ra để xem nó bắt chờ bao lâu (thường nó viết trong này)
            error_body = response.text
            log(f" ⛔ BỊ CHẶN (429)! Nội dung từ Server: {error_body}")
            
            # 2. Đánh dấu Job là FAILED ngay lập tức (Để Worker không bị kẹt)
            update_job_status(job_id, 'failed', f"Rate Limit 429. Server said: {error_body[:100]}...")
            
            # 3. Đóng dòng lịch sử chạy
            conn = get_db()
            cur = conn.cursor()
            cur.execute("UPDATE job_history SET end_time=NOW(), status='Failed' WHERE id=%s", (hist_id,))
            conn.commit()
            conn.close()
            
            # 4. QUAN TRỌNG: Return luôn để thoát khỏi hàm, giải phóng Worker
            log(" 🛑 Dừng Job hiện tại để bảo toàn lực lượng. Vui lòng kiểm tra log và thử lại sau.")
            return

        else:
            log(f"  ❌ HTTP Error {response.status_code}")
            update_job_status(job_id, 'failed', f"HTTP {response.status_code}")
            # Đóng History vì lỗi
            conn = get_db()
            cur = conn.cursor()
            cur.execute("UPDATE job_history SET end_time=NOW(), status='Failed' WHERE id=%s", (hist_id,))
            conn.commit()
            conn.close()

    except Exception as e:
        log(f"  ❌ Exception: {str(e)}")
        update_job_status(job_id, 'failed', str(e))
        conn = get_db()
        cur = conn.cursor()
        cur.execute("UPDATE job_history SET end_time=NOW(), status='Failed' WHERE id=%s", (hist_id,))
        conn.commit()
        conn.close()
    
    finally:
        set_system_busy(False)

def run_worker_loop():
    print("🚀 Worker Loop Started...")
    while True:
        try:
            worker_process_jobs()
        except Exception as e:
            print(f"❌ Worker Loop Error: {e}")
        time.sleep(20)

# ==========================================
# PHẦN 3: SCHEDULER THÔNG MINH 
# ==========================================
def run_scheduler_loop():
    print("🚀 Smart Scheduler Started (Anchor Time & Skip Logic)...")
    while True:
        try:
            now = datetime.now()
            conn = get_db()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT * FROM apps WHERE is_active = true") 
            apps = cur.fetchall()

            for app in apps:
                app_id = app['id']
                interval_minutes = app.get('interval_minutes', 60) 
                if not interval_minutes: interval_minutes = 60

                sch_time_str = app.get('schedule_time', '00:00')
                try:
                    h, m = map(int, sch_time_str.split(':'))
                    anchor_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
                except:
                    anchor_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
                
                if anchor_time > now:
                    anchor_time = anchor_time - timedelta(days=1)
                
                diff = now - anchor_time
                cycles_passed = int(diff.total_seconds() // (interval_minutes * 60))
                
                expected_run_time = anchor_time + timedelta(minutes=cycles_passed * interval_minutes)
                
                time_since_expected = (now - expected_run_time).total_seconds()
                is_time_to_run = 0 <= time_since_expected < 65

                if is_time_to_run:
                    cur.execute("SELECT count(*) as count FROM etl_jobs WHERE app_id = %s AND created_at > %s", 
                                (app_id, now - timedelta(minutes=2)))
                    
                    if cur.fetchone()['count'] == 0:
                        print(f"⏰ Triggering Schedule for App #{app_id} at {now.strftime('%H:%M:%S')}")
                        
                        if is_system_busy():
                            print(f"⚠️ SKIPPING Auto Schedule for App #{app_id} - System is BUSY")
                            cur.execute("""
                                INSERT INTO job_history (app_id, start_time, status, run_type, logs) 
                                VALUES (%s, NOW(), 'Skipped', 'schedule', 'Skipped due to System Busy (Conflict)')
                            """, (app_id,))
                            conn.commit()
                        else:
                            delay_minutes = 90
                            end_time_vn = now - timedelta(minutes=delay_minutes)
                            start_time_vn = end_time_vn - timedelta(minutes=interval_minutes)
                            
                            end_time_utc = end_time_vn - timedelta(hours=7)
                            start_time_utc = start_time_vn - timedelta(hours=7)
                            
                            date_until = end_time_utc.strftime('%Y-%m-%d %H:%M:%S')
                            date_since = start_time_utc.strftime('%Y-%m-%d %H:%M:%S')

                            print(f"  🎫 Creating Job: VN[{start_time_vn.strftime('%H:%M')} - {end_time_vn.strftime('%H:%M')}] -> UTC[{date_since} - {date_until}]")
                            create_etl_job(app_id, date_since, date_until)

            cur.close()
            conn.close()
        except Exception as e:
            print(f"❌ Scheduler Error: {e}")
        
        time.sleep(60)

# PHẦN 4: LOGIC CHẠY TAY (MANUAL) - [FIXED RETRY LOGIC]
def perform_manual_etl(app_id, run_type='manual', is_demo=False, retry_job_id=None):
    if is_system_busy():
        print(f"❌ System BUSY. Skip run for App {app_id}.")
        return

    set_system_busy(True, app_id, run_type)
    hist_id = None

    try:
        conn = get_db()
        if not conn: return
        
        # 1. TẠO RECORD HISTORY
        msg_start = f"🚀 Starting {run_type.upper()} run..."
        if retry_job_id: msg_start += f" (Retry of Job #{retry_job_id})"
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO job_history (app_id, start_time, status, run_type, logs, total_events) 
            VALUES (%s, NOW(), 'Running', %s, %s, 0) 
            RETURNING id
        """, (app_id, run_type, f"[{datetime.now().strftime('%H:%M:%S')}] {msg_start}"))
        hist_id = cur.fetchone()[0]
        conn.commit()
        
        stop_event = threading.Event()
        JOB_STOP_EVENTS[hist_id] = stop_event

        def log(msg):
            print(msg)
            append_log_to_db(hist_id, msg)

        # 2. LẤY CẤU HÌNH APP
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM apps WHERE id=%s", (app_id,))
        app = cur.fetchone()
        
        if not app:
            log("❌ Error: App ID not found.")
            cur.execute("UPDATE job_history SET end_time=NOW(), status='Failed' WHERE id=%s", (hist_id,))
            conn.commit()
            return 

        # 3. CẤU HÌNH THỜI GIAN
        date_since = None
        date_until = None
        
        # --- CASE 1: STRICT RETRY (Logic V85 - Vét cạn Regex) ---
        if run_type == 'retry' and retry_job_id:
            cur.execute("SELECT logs, start_time FROM job_history WHERE id = %s", (retry_job_id,))
            old_row = cur.fetchone()
            
            if old_row and old_row['logs']:
                logs = old_row['logs']
                
                # A. Tìm 2 chuỗi ngày giờ đầy đủ (YYYY-MM-DD HH:MM:SS)
                # Bất kể nó nằm trong dấu [], (), hay sau dấu :
                full_timestamps = re.findall(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", logs)
                
                if len(full_timestamps) >= 2:
                    # Lấy 2 mốc cuối cùng tìm được (Thường là Scanning Window)
                    date_since = full_timestamps[-2]
                    date_until = full_timestamps[-1]
                    log(f"🔙 RETRY V85: Found exact window: {date_since} -> {date_until}")
                
                # B. Nếu không có ngày, tìm 2 chuỗi giờ (HH:MM)
                elif old_row['start_time']:
                    short_times = re.findall(r"(\d{2}:\d{2})", logs)
                    if len(short_times) >= 2:
                        # Lấy ngày gốc của Job cũ
                        base_date = old_row['start_time'].strftime('%Y-%m-%d')
                        # Ghép ngày + giờ tìm được
                        # Lưu ý: short_times có thể bắt nhầm giờ trong log message, nên lấy 2 cái cuối
                        date_since = f"{base_date} {short_times[-2]}:00"
                        date_until = f"{base_date} {short_times[-1]}:00"
                        log(f"🔙 RETRY V85 (Short): Reconstructed: {date_since} -> {date_until}")

            # [STRICT MODE] Nếu vẫn không tìm thấy -> BÁO LỖI
            if not date_since:
                log(f"❌ RETRY FAILED: Cannot find time patterns in logs of Job #{retry_job_id}.")
                cur.execute("UPDATE job_history SET end_time=NOW(), status='Failed' WHERE id=%s", (hist_id,))
                conn.commit()
                return

        # --- CASE 2: DEMO / MANUAL ---
        if not date_since:
            now = datetime.now()
            delay_minutes = 45 if run_type == 'demo' else 90
            duration_minutes = 15 if run_type == 'demo' else 60
            
            end_dt = now - timedelta(minutes=delay_minutes)
            start_dt = end_dt - timedelta(minutes=duration_minutes)
            
            date_since = start_dt.strftime('%Y-%m-%d %H:%M:%S')
            date_until = end_dt.strftime('%Y-%m-%d %H:%M:%S')
            log(f"⚙️ MANUAL MODE: {date_since} -> {date_until}")

        log(f" 🕒 Scanning Window: {date_since} -> {date_until}")
        
        # 4. GỌI API APPMETRICA (Dùng .strip() an toàn)
        clean_app_id = str(app['app_id']).strip()
        clean_token = str(app['api_token']).strip()

        url = "https://api.appmetrica.yandex.com/logs/v1/export/events.json"
        params = {
            "application_id": clean_app_id,
            "date_since": date_since,
            "date_until": date_until,
            "fields": "event_name,event_timestamp,event_json",
            "limit": 1000000
        }
        headers = {"Authorization": f"OAuth {clean_token}"}
        
        status = "Failed"; total_events = 0
        
        for i in range(18): # 18 retries
            if stop_event.is_set():
                log("🛑 USER STOPPED PROCESS.")
                status = "Cancelled"; break

            log(f"📡 Requesting AppMetrica (Attempt {i+1}/18)...")
            resp = requests.get(url, params=params, headers=headers)
            
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                total_events = len(data)
                log(f"✅ Success! Received {total_events} events. Importing...")
                
                conn_insert = get_db(); cur_insert = conn_insert.cursor()
                values = []
                for d in data:
                    try: ts = datetime.fromtimestamp(int(d.get('event_timestamp')))
                    except: ts = datetime.now()
                    values.append((app_id, d.get('event_name', 'unknown'), json.dumps(d), 1, ts))
                
                cur_insert.executemany("INSERT INTO event_logs (app_id, event_name, event_json, count, created_at) VALUES (%s,%s,%s,%s,%s)", values)
                conn_insert.commit(); conn_insert.close()
                
                status = "Success"; log(f"🎉 Done. Imported {total_events} events."); break
            
            elif resp.status_code == 202:
                log(f"⏳ Server 202. Waiting 180s..."); 
                if stop_event.wait(180): status = "Cancelled"; break
            else:
                log(f"❌ Error {resp.status_code}"); status = "Failed"; break
        
        # Cập nhật kết quả cuối cùng
        conn_end = get_db(); cur_end = conn_end.cursor()
        cur_end.execute("UPDATE job_history SET end_time=NOW(), status=%s, total_events=%s WHERE id=%s", (status, total_events, hist_id))
        conn_end.commit(); conn_end.close()
    
    except Exception as e:
        log(f"❌ Critical Error: {str(e)}")
    finally:
        set_system_busy(False)

# PHẦN 5: API ENDPOINTS (ĐÃ CẬP NHẬT DASHBOARD)
@app.route("/monitor/history", methods=['GET'])
def get_history():
    app_id = request.args.get('app_id') 
    # [MỚI] Lấy tham số phân trang
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 30)) # Mặc định 30 dòng/trang
    except:
        page = 1; limit = 30
        
    offset = (page - 1) * limit
    conn = get_db()
    if not conn: return jsonify([])
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Xây dựng mệnh đề WHERE
        where_clause = ""
        params_count = []
        if app_id: 
            where_clause = "WHERE h.app_id = %s"
            params_count.append(app_id)

        # 2. Đếm tổng số records (để tính số trang)
        cur.execute(f"SELECT COUNT(*) as total FROM job_history h {where_clause}", tuple(params_count))
        total_records = cur.fetchone()['total']
        total_pages = (total_records + limit - 1) // limit

        # 3. Lấy dữ liệu phân trang
        query = f"""
            SELECT h.*, a.name as app_name 
            FROM job_history h 
            JOIN apps a ON h.app_id = a.id 
            {where_clause}
            ORDER BY h.start_time DESC 
            LIMIT %s OFFSET %s
        """
        # Copy params từ count sang và thêm limit/offset
        params_data = params_count + [limit, offset]
        
        cur.execute(query, tuple(params_data))
        res = cur.fetchall()

        # --- [FIX LỖI TIMEZONE VÀ DURATION] ---
        # Tính toán Duration và Format lại Time để tránh Frontend tự cộng +7 tiếng
        for row in res:
            # 1. Tính Duration
            duration_str = "..."
            if row['start_time'] and row['end_time']:
                diff = row['end_time'] - row['start_time']
                total_seconds = int(diff.total_seconds())
                minutes = total_seconds // 60
                seconds = total_seconds % 60
                duration_str = f"{minutes} min {seconds} sec"
            elif row['status'] == 'Running':
                 duration_str = "Running..."
            
            row['duration'] = duration_str

            # 2. Fix Timezone: Chuyển datetime thành string cứng
            # Để Frontend hiển thị y nguyên giờ của Server (Local VN)
            if row['start_time']:
                row['start_time'] = row['start_time'].strftime('%d/%m/%Y %H:%M:%S')
            if row['end_time']:
                row['end_time'] = row['end_time'].strftime('%d/%m/%Y %H:%M:%S')

        return jsonify({
            "data": res,
            "pagination": {
                "current_page": page,
                "total_pages": total_pages,
                "total_records": total_records
            }
        })
    except Exception as e:
        print(f"History Error: {e}")
        return jsonify({"data": [], "pagination": {}})    
    finally: conn.close()

@app.route("/monitor/purge", methods=['DELETE'])
def purge_history():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM job_history")
        conn.commit()
        return jsonify({"msg": "History Cleared"})
    finally: conn.close()

@app.route("/apps", methods=['GET', 'POST'])
def handle_apps():
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if request.method == 'GET':
            cur.execute("SELECT * FROM apps ORDER BY id ASC")
            return jsonify(cur.fetchall())
        else:
            d = request.json
            cur.execute("INSERT INTO apps (name, app_id, api_token, is_active, schedule_time, interval_minutes) VALUES (%s, %s, %s, %s, %s, %s)", 
                        (d['name'], d['app_id'], d['api_token'], d['is_active'], d.get('schedule_time', '12:00'), d.get('interval_minutes', 60)))
            conn.commit()
            return jsonify({"msg": "Created"})
    finally: conn.close()

@app.route("/apps/<int:id>", methods=['PUT', 'DELETE'])
def update_app(id):
    conn = get_db()
    try:
        cur = conn.cursor()
        if request.method == 'PUT':
            d = request.json
            cur.execute("UPDATE apps SET name=%s, app_id=%s, api_token=%s, is_active=%s, schedule_time=%s, interval_minutes=%s WHERE id=%s", 
                        (d['name'], d['app_id'], d['api_token'], d['is_active'], d.get('schedule_time', '12:00'), d.get('interval_minutes', 60), id))
            conn.commit()
            return jsonify({"msg": "Updated"})
        elif request.method == 'DELETE':
            cur.execute("DELETE FROM event_logs WHERE app_id=%s", (id,))
            cur.execute("DELETE FROM job_history WHERE app_id=%s", (id,))
            cur.execute("DELETE FROM etl_jobs WHERE app_id=%s", (id,))
            cur.execute("DELETE FROM apps WHERE id=%s", (id,))
            conn.commit()
            return jsonify({"msg": "Deleted"})
    finally: conn.close()

@app.route("/etl/run/<int:app_id>", methods=['POST'])
def run_etl_api(app_id):
    # --- CODE MỚI ---
    data = request.json
    run_type = data.get('run_type', 'manual') # Lấy loại chạy (manual/retry/demo)
    retry_job_id = data.get('retry_job_id')   # Lấy ID của job cũ nếu là retry
    
    is_demo = (run_type == 'demo')
    
    if is_system_busy():
         return jsonify({"status": "error", "message": "System is busy processing another job. Please skip this cycle."}), 409

    # Truyền thêm retry_job_id vào hàm xử lý
    threading.Thread(target=perform_manual_etl, args=(app_id, run_type, is_demo, retry_job_id)).start()
    return jsonify({"status": "started", "mode": run_type})

@app.route("/dashboard/<int:app_id>", methods=['GET'])
def get_dashboard(app_id):
    conn = get_db()
    if not conn: return jsonify({"success": False, "error": "DB Connection failed"}), 500
    
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. KHỞI TẠO BIẾN AN TOÀN (FIX LỖI 'variable not defined')
        booster_config_list = [] 
        real_events = ["iapSuccess", "firstIAP", "iapPurchase"]
        start_events = ["missionStart", "missionStart_Daily", "level_start", "level_loading_start", "level_first_start"]
        fail_events = ["missionFail", "missionFail_Daily", "level_fail", "level_lose"]
        
        try:
            config = get_app_config(cur, app_id)
            if config:
                # Cập nhật nếu config có dữ liệu
                c_booster = config.get('boosters')
                if isinstance(c_booster, list): booster_config_list = c_booster
                
                c_real = config.get('events', {}).get('transaction', {}).get('real_currency')
                if c_real: real_events = c_real
        except: pass

        # Lấy danh sách key booster
        booster_keys = [b['key'] for b in booster_config_list if isinstance(b, dict) and 'key' in b]
        if not booster_keys:
            booster_keys = ["booster_Hammer", "booster_Magnet", "booster_Add", "booster_Unlock", "booster_Clear", "revive_boosterClear", "booster_bubble", "booster_shuffle", "booster_ufo"]

        # 2. FILTER TIME
        where_clause = "WHERE app_id = %s"
        params = [app_id]
        if start_date: 
            where_clause += " AND created_at >= %s"
            params.append(start_date + " 00:00:00")
        if end_date: 
            where_clause += " AND created_at <= %s"
            params.append(end_date + " 23:59:59")

        # 3. DOANH THU (Regex Extract)
        cur.execute(r"""
            SELECT COALESCE(SUM(
                COALESCE(SUBSTRING(event_json FROM '"coin_spent"\s*:\s*"?(\d+)"?')::numeric, 0)
            ), 0)::int as real_revenue
            FROM event_logs
            """ + where_clause + r""" 
            AND event_name = ANY(%s) 
        """, tuple(params + [real_events]))
        real_revenue = cur.fetchone()['real_revenue']

        # 4. TỔNG TIÊU COIN (Regex Extract)
        cur.execute(r"""
            SELECT SUM(
                 COALESCE(SUBSTRING(event_json FROM '"coin_spent"\s*:\s*"?(\d+)"?')::int, 0)
            )::int as virtual_sink
            FROM event_logs
            """ + where_clause + r"""
            AND NOT (event_name = ANY(%s))
        """, tuple(params + [real_events]))
        virtual_sink = cur.fetchone()['virtual_sink'] or 0

        # 5. TOTAL PLAYS
        cur.execute(f"SELECT COUNT(*)::int as count FROM event_logs {where_clause} AND event_name = ANY(%s)", tuple(params + [start_events]))
        total_plays = cur.fetchone()['count'] or 0

        # 6. FAIL RATE
        cur.execute(f"SELECT COUNT(*)::int as count FROM event_logs {where_clause} AND event_name = ANY(%s)", tuple(params + [fail_events]))
        real_fail_count = cur.fetchone()['count'] or 0
        fail_rate = round((real_fail_count / total_plays) * 100, 1) if total_plays > 0 else 0.0

        # 7. CHART MAIN
        cur.execute(f"SELECT event_name as name, COUNT(*)::int as value FROM event_logs {where_clause} GROUP BY event_name ORDER BY value DESC", tuple(params))
        chart_data = cur.fetchall()

        # 8. BOOSTER REVENUE (Manual Count bằng SQL)
        # Vì Regex trong Group By phức tạp, ta lấy raw về xử lý một chút hoặc dùng count đơn giản
        booster_stats = []
        if booster_keys:
            # Map giá
            PRICE_MAP = {}
            NAME_MAP = {}
            for b in booster_config_list:
                if isinstance(b, dict):
                    k = b.get('key')
                    PRICE_MAP[k] = b.get('price', 100)
                    NAME_MAP[k] = b.get('name', k)

            # Query đếm số lần xuất hiện của key trong chuỗi JSON
            # Lưu ý: Cách này đếm số dòng có chứa key đó
            for key in booster_keys:
                # Regex tìm "key": so_luong (số lượng > 0)
                # Ví dụ: "booster_Hammer": 1
                cur.execute(r"""
                    SELECT COUNT(*) as cnt 
                    FROM event_logs 
                    """ + where_clause + r""" 
                    AND event_json ~ (%s || '\s*:\s*[1-9]')
                """, tuple(params + [key]))
                
                count = cur.fetchone()['cnt']
                if count > 0:
                    price = PRICE_MAP.get(key, 100)
                    name = NAME_MAP.get(key, key)
                    booster_stats.append({
                        "name": name,
                        "value": count,
                        "revenue": count * price,
                        "price": price
                    })
            
            booster_stats.sort(key=lambda x: x['revenue'], reverse=True)

        return jsonify({
            "success": True,
            "overview": {
                "cards": {
                    "revenue": real_revenue,      
                    "active_users": total_plays,
                    "avg_fail_rate": fail_rate,
                    "total_spent": virtual_sink  
                },
                "chart_main": chart_data,
                "booster_chart": booster_stats
            }
        })

    except Exception as e:
        print(f"Error dashboard V93: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally: conn.close()

@app.route("/api/levels/<int:app_id>", methods=['GET'])
def get_levels(app_id):
    conn = get_db()
    if not conn: return jsonify([])
    try:
        cur = conn.cursor()
        # Lấy TOÀN BỘ dữ liệu để lọc chính xác (Không dùng LIMIT)
        cur.execute("SELECT event_json FROM event_logs WHERE app_id = %s", (app_id,))
        rows = cur.fetchall()
        
        levels = set()
        for r in rows:
            # Dùng mũi khoan vạn năng V95
            data = universal_flatten(r[0])
            
            # Tìm Level ID ở mọi key có thể (Game 1 & 2)
            lvl = data.get('levelID') or data.get('level_display') or data.get('missionID')
            
            if lvl is not None:
                # Lọc lấy số
                digits = ''.join(filter(str.isdigit, str(lvl)))
                if digits:
                    l = int(digits)
                    if l <= 2000: levels.add(l)
        
        sorted_levels = sorted(list(levels))
        return jsonify([str(l) for l in sorted_levels])
    except Exception as e:
        print(f"Error get_levels V95: {e}")
        return jsonify([])
    finally: conn.close()

@app.route("/dashboard/<int:app_id>/level-detail", methods=['GET'])
def get_level_detail(app_id):
    level_id = request.args.get('level_id') # Level người dùng chọn từ menu
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try: page = int(request.args.get('page', 1)); limit = int(request.args.get('limit', 50))
    except: page=1; limit=50
    offset = (page - 1) * limit

    conn = get_db()
    if not conn: return jsonify({"success": False}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Config Booster Map (Fallback)
        PRICE_MAP = {"Hammer": 120, "Magnet": 80, "Add": 60, "Unlock": 190, "Clear": 120}
        DISPLAY_MAP = {"Hammer": "Hammer 🔨", "Magnet": "Magnet 🧲", "Add": "Add Moves ➕"}
        
        # Cố gắng load config xịn từ DB
        try:
            cfg = get_app_config(cur, app_id)
            if cfg and 'boosters' in cfg:
                for b in cfg['boosters']:
                    if isinstance(b, dict):
                        k = b.get('key',''); nm = b.get('name',''); pr = b.get('price', 100)
                        cl = k.replace('booster_', '').replace('revive_', '')
                        PRICE_MAP[cl] = pr; PRICE_MAP[k] = pr
                        DISPLAY_MAP[cl] = nm; DISPLAY_MAP[k] = nm
        except: pass

        # LẤY TOÀN BỘ LOG TRONG KHOẢNG THỜI GIAN (Lọc Level bằng Python cho chắc)
        where = "WHERE app_id = %s"; params = [app_id]
        if start_date: where += " AND created_at >= %s"; params.append(start_date + " 00:00:00")
        if end_date: where += " AND created_at <= %s"; params.append(end_date + " 23:59:59")
        
        # Sắp xếp mới nhất trước
        cur.execute(f"SELECT created_at, event_name, event_json FROM event_logs {where} ORDER BY created_at DESC", tuple(params))
        all_rows = cur.fetchall()

        # --- XỬ LÝ PYTHON ---
        target_lvl = str(level_id)
        filtered_rows = []
        
        metrics = {"start":0, "win":0, "fail":0, "spend":0, "rev":0}
        b_counts = {}
        
        start_set = {"missionStart", "missionStart_Daily", "level_start", "level_loading_start"}
        win_set = {"missionComplete", "missionComplete_Daily", "level_win"}
        fail_set = {"missionFail", "missionFail_Daily", "level_fail", "level_lose"}

        for r in all_rows:
            # MŨI KHOAN V95
            data = universal_flatten(r['event_json'])
            
            # Kiểm tra Level (Chấp nhận cả số và chuỗi)
            row_lvl = str(data.get('levelID') or data.get('level_display') or data.get('missionID') or "")
            if row_lvl != target_lvl: continue
            
            # Nếu khớp level -> Lưu lại để xử lý tiếp
            r_dict = dict(r); r_dict['parsed'] = data
            filtered_rows.append(r_dict)
            
            evt = r['event_name']
            if evt in start_set: metrics['start'] += 1
            elif evt in win_set: metrics['win'] += 1
            elif evt in fail_set: metrics['fail'] += 1
            
            # Tiền
            money = int(data.get('coin_spent') or data.get('coin_cost') or 0)
            if money > 0:
                metrics['spend'] += 1
                metrics['rev'] += money
            
            # Booster (Quét tất cả key)
            for k, v in data.items():
                if ('booster' in k or 'revive' in k) and str(v).isdigit() and int(v) > 0:
                    clean = k.replace('booster_', '').replace('revive_', '')
                    b_counts[clean] = b_counts.get(clean, 0) + int(v)

        # TÍNH TOÁN METRICS CUỐI CÙNG
        real_plays = metrics['win'] + metrics['fail']
        if real_plays == 0: real_plays = metrics['start']
        win_rate = round((metrics['win']/real_plays)*100, 1) if real_plays > 0 else 0
        
        # Danh sách Booster
        b_list = []
        for k, c in b_counts.items():
            nm = DISPLAY_MAP.get(k, k.capitalize())
            pr = PRICE_MAP.get(k, 100)
            b_list.append({"item_name": nm, "usage_count": c, "revenue": c*pr, "price": pr, "type": "Used"})
        b_list.sort(key=lambda x: x['revenue'], reverse=True)
        
        top_item = b_list[0]['item_name'] if b_list else "None"
        arpu = sum(x['revenue'] for x in b_list)

        final_metrics = { "total_plays": real_plays, "win_rate": win_rate, "arpu": arpu, "avg_balance": 0, "top_item": top_item }
        funnel = [
            {"event_type": "START", "count": real_plays, "revenue": 0},
            {"event_type": "WIN", "count": metrics['win'], "revenue": 0},
            {"event_type": "SPEND", "count": metrics['spend'], "revenue": metrics['rev']},
            {"event_type": "FAIL", "count": metrics['fail'], "revenue": 0}
        ]

        # PHÂN TRANG (Pagination)
        total_rec = len(filtered_rows)
        paged_data = filtered_rows[offset : offset + limit]
        
        proc_logs = []
        for r in paged_data:
            d = r['parsed']
            u = d.get('userID') or d.get('uuid') or "Guest"
            dt = []
            if d.get('coin_spent'): dt.append(f"💸 -{d['coin_spent']}")
            for k,v in d.items():
                if ('booster' in k or 'revive' in k) and int(v) > 0: 
                    dt.append(f"⚡ {k.replace('booster_', '')} x{v}")
            
            proc_logs.append({
                "time": r['created_at'].strftime('%H:%M:%S %d/%m'),
                "user_id": str(u)[:15]+"..",
                "event_name": r['event_name'],
                "coin_spent": d.get('coin_spent', 0),
                "item_name": " | ".join(dt) if dt else "-"
            })

        return jsonify({
            "success": True,
            "metrics": final_metrics, "funnel": funnel, "booster_usage": b_list, 
            "logs": { "data": proc_logs, "pagination": { "current": page, "total_pages": (total_rec+limit-1)//limit, "total_records": total_rec } }
        })

    except Exception as e:
        print(f"Level Detail Error V95: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally: conn.close()

@app.route("/dashboard/<int:app_id>/strategic", methods=['GET'])
def get_strategic_overview(app_id):
    conn = get_db()
    if not conn: return jsonify({"success": False, "error": "DB error"}), 500

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. DANH SÁCH SỰ KIỆN TỔNG HỢP (Game 1 + Game 2)
        start_set = {"missionStart", "missionStart_Daily", "level_start", "level_loading_start", "level_first_start"}
        fail_set = {"missionFail", "missionFail_Daily", "level_fail", "level_lose"}
        
        # 2. LẤY DỮ LIỆU THÔ (FULL SCAN)
        where = "WHERE app_id = %s"; params = [app_id]
        if start_date: where += " AND created_at >= %s"; params.append(start_date + " 00:00:00")
        if end_date: where += " AND created_at <= %s"; params.append(end_date + " 23:59:59")
        
        # Chỉ lấy cột cần thiết để tối ưu tốc độ
        cur.execute(f"SELECT event_name, event_json FROM event_logs {where}", tuple(params))
        rows = cur.fetchall()
        
        # 3. TÍNH TOÁN (PYTHON AGGREGATION)
        stats = {} # {lvl: {plays, fails, rev}}

        for r in rows:
            # BƯỚC QUAN TRỌNG: KHOAN SÂU DỮ LIỆU
            data = universal_flatten(r['event_json'])
            
            # Tìm Level
            lvl_raw = data.get('levelID') or data.get('level_display') or data.get('missionID')
            if not lvl_raw: continue
            
            try:
                lvl_num = int(''.join(filter(str.isdigit, str(lvl_raw))))
                if lvl_num > 2000: continue
            except: continue
            
            if lvl_num not in stats: stats[lvl_num] = {"plays": 0, "fails": 0, "rev": 0}
            
            evt = r['event_name']
            if evt in start_set: stats[lvl_num]['plays'] += 1
            elif evt in fail_set: stats[lvl_num]['fails'] += 1
            
            # Cộng tiền (Ưu tiên coin_spent, dự phòng coin_cost)
            money = data.get('coin_spent') or data.get('coin_cost') or 0
            try: stats[lvl_num]['rev'] += float(money)
            except: pass

        # 4. FORMAT CHART
        chart = []
        for lvl, val in stats.items():
            if val['plays'] > 0 or val['rev'] > 0 or val['fails'] > 0:
                fr = round((val['fails'] / val['plays']) * 100, 1) if val['plays'] > 0 else 0
                if fr > 100: fr = 100.0
                chart.append({
                    "name": f"Lv.{lvl}", "level_index": lvl,
                    "revenue": val['rev'], "fail_rate": fr, "plays": val['plays']
                })

        chart.sort(key=lambda x: x['level_index'])
        return jsonify({"success": True, "balance_chart": chart})

    except Exception as e:
        print(f"Strategic Error V95: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally: conn.close()

# --- BỔ SUNG API: XÓA 1 DÒNG & STOP JOB ---
@app.route("/monitor/history/<int:id>", methods=['DELETE'])
def delete_single_history(id):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM job_history WHERE id = %s", (id,))
        conn.commit()
        return jsonify({"success": True, "msg": f"Deleted history #{id}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally: conn.close()

@app.route("/etl/stop/<int:hist_id>", methods=['POST'])
def stop_etl_process(hist_id):
    # API này dùng để đánh dấu job là Cancelled trên Database
    # Nó cũng cố gắng reset trạng thái bận của hệ thống nếu cần
    conn = get_db()
    try:
        # 1. Kích hoạt cờ dừng để Worker đang chạy tự thoát
        if hist_id in JOB_STOP_EVENTS:
            print(f"🛑 Sending STOP signal to Job #{hist_id}...")
            JOB_STOP_EVENTS[hist_id].set() # Đánh thức Worker ngay lập tức
        cur = conn.cursor()
        # Cập nhật DB
        cur.execute("""
            UPDATE job_history 
            SET status = 'Cancelled', end_time = NOW(), logs = logs || E'\n[USER MANUAL STOP]'
            WHERE id = %s AND status IN ('Running', 'Processing')
        """, (hist_id,))
        conn.commit()

        # 3. Reset hệ thống nếu cần
        if is_system_busy():
            set_system_busy(False)
            
        return jsonify({"success": True, "msg": f"Stop signal sent to Job #{hist_id}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally: conn.close()

# --- [V41 FIX] API CẤU HÌNH ĐỘNG (DÙNG JSON DB) ---
@app.route("/apps/<int:app_id>/analytics-config", methods=['GET', 'POST'])
def handle_analytics_config(app_id):
    conn = get_db()
    if not conn: return jsonify({"success": False, "error": "DB Connection Failed"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # --- 1. LẤY CẤU HÌNH (GET) ---
        if request.method == 'GET':
            # Query mới: Chỉ lấy cột config_json
            cur.execute("SELECT config_json FROM analytics_config WHERE app_id = %s", (app_id,))
            row = cur.fetchone()
            
            if row and row['config_json']:
                # Trả về JSON chuẩn cho Frontend
                return jsonify(row['config_json'])
            else:
                # Nếu chưa có trong DB, trả về config mặc định từ hàm get_app_config
                # (Lưu ý: Bạn phải đảm bảo hàm get_app_config ở đầu file đã sửa tên bảng thành analytics_config nhé)
                return jsonify(get_app_config(cur, app_id))

        # --- 2. LƯU CẤU HÌNH (POST) ---
        elif request.method == 'POST':
            new_config = request.json # Frontend gửi lên toàn bộ cục JSON settings
            
            # Chuyển Dict thành String JSON để lưu vào DB
            config_str = json.dumps(new_config)

            # Lưu thẳng vào cột config_json (Gọn nhẹ hơn logic cũ rất nhiều)
            cur.execute("""
                INSERT INTO analytics_config (app_id, config_json, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (app_id) DO UPDATE SET 
                    config_json = EXCLUDED.config_json,
                    updated_at = NOW()
            """, (app_id, config_str))
            
            conn.commit()
            return jsonify({"success": True, "msg": "Configuration Saved Successfully (V40 JSON Mode)"})
            
    except Exception as e:
        print(f"❌ Analytics Config Error: {e}")
        conn.rollback() # Chống kẹt transaction
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

# --- API CHẠY ETL (TỔNG HỢP DỮ LIỆU) ---
@app.route("/api/run-etl/<int:app_id>", methods=['POST'])
def trigger_etl_process(app_id):
    # Chạy trong thread riêng để không block server
    threading.Thread(target=run_etl_pipeline, args=(app_id,)).start()
    return jsonify({"status": "started", "message": "ETL process started in background"})

# --- API MỚI: TRA CỨU DỮ LIỆU THÔ (DATA EXPLORER) ---
@app.route("/events/search", methods=['GET'])
def search_events():
    try:
        app_id = request.args.get('app_id')
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 20))
        
        # Các bộ lọc
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        event_name = request.args.get('event_name')
        keyword = request.args.get('keyword') # Tìm UserID hoặc nội dung bất kỳ trong JSON

        if not app_id:
            return jsonify({"success": False, "error": "Missing app_id"}), 400

        conn = get_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Xây dựng câu WHERE động
        where_clauses = ["app_id = %s"]
        params = [app_id]

        if start_date:
            where_clauses.append("created_at >= %s")
            params.append(start_date + " 00:00:00")
        
        if end_date:
            where_clauses.append("created_at <= %s")
            params.append(end_date + " 23:59:59")

        if event_name and event_name.strip():
            where_clauses.append("event_name = %s")
            params.append(event_name)

        if keyword and keyword.strip():
            # Kỹ thuật tìm kiếm trong JSON (Chuyển JSON thành Text để tìm)
            where_clauses.append("event_json::text ILIKE %s")
            params.append(f"%{keyword}%")

        full_where = " WHERE " + " AND ".join(where_clauses)

        # 2. Đếm tổng số dòng (để làm phân trang 1/100...)
        count_query = f"SELECT COUNT(*) as total FROM event_logs {full_where}"
        cursor.execute(count_query, tuple(params))
        total_records = cursor.fetchone()['total']
        total_pages = (total_records + limit - 1) // limit

        # 3. Lấy dữ liệu phân trang
        offset = (page - 1) * limit
        data_query = f"""
            SELECT 
                id, 
                event_name, 
                to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') as created_at,
                event_json -- Lấy nguyên cục JSON về để Frontend hiển thị đẹp
            FROM event_logs 
            {full_where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        # Thêm limit/offset vào params
        params.extend([limit, offset])
        
        cursor.execute(data_query, tuple(params))
        rows = cursor.fetchall()

        # 4. Trích xuất sơ bộ User ID để hiển thị ra ngoài bảng (cho tiện nhìn)
        for row in rows:
            try:
                # Parse JSON string thành Dict
                import json
                raw = row['event_json']
                # Xử lý double-encoded nếu có
                if isinstance(raw, str):
                    parsed = json.loads(raw)
                    # Nếu bên trong lại có key 'event_json' dạng string
                    if isinstance(parsed, dict) and 'event_json' in parsed and isinstance(parsed['event_json'], str):
                        inner = json.loads(parsed['event_json'])
                        parsed.update(inner)
                    row['event_json'] = parsed # Gán lại object đã sạch
                
                # --- [LOGIC MỚI] TẠO CỘT KEY INFO ---
                # Thay vì lấy UserID, ta lấy thông tin ngữ cảnh quan trọng hơn
                data = row['event_json']
                info_parts = []
                
                # 1. Nếu có thông tin Level/Mission -> Lấy ngay
                if 'levelID' in data: info_parts.append(f"Lv.{data['levelID']}")
                if 'missionID' in data: info_parts.append(f"Ms.{data['missionID']}")
                
                # 2. Nếu có thông tin Tiền/Giá -> Lấy ngay
                if 'coin_cost' in data: info_parts.append(f"-{data['coin_cost']} Coin")
                if 'coin_price' in data: info_parts.append(f"-{data['coin_price']} Coin")
                if 'revenue' in data: info_parts.append(f"+{data['revenue']} USD")
                
                # 3. Nếu có thông tin Item/Booster
                if 'item_name' in data: info_parts.append(data['item_name'])
                
                # Gán vào biến mới để trả về Frontend
                row['key_info'] = " | ".join(info_parts) if info_parts else "..."
            except:
                row['key_info'] = '-'
                row['event_json'] = {}

        return jsonify({
            "success": True,
            "data": rows,
            "pagination": {
                "current_page": page,
                "total_pages": total_pages,
                "total_records": total_records,
                "limit": limit
            }
        })

    except Exception as e:
        print(f"Search Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    t1 = threading.Thread(target=run_scheduler_loop)
    t1.daemon = True
    t1.start()

    t2 = threading.Thread(target=run_worker_loop)
    t2.daemon = True
    t2.start()

    print("🚀 SYSTEM READY: Smart Scheduler & Worker Threads started...")
    app.run(host='0.0.0.0', port=8080, debug=True, use_reloader=False)