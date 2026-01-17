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
            
            # --- [SỬA ĐỔI QUAN TRỌNG TẠI ĐÂY] ---
            # Thay vì json.loads đơn thuần, ta dùng hàm thông minh
            raw_json = smart_parse_json(e.get("event_json"))
            # ------------------------------------

            level_id = raw_json.get("levelID") or raw_json.get("missionID")
            user_id = raw_json.get("userID") or "Guest"
            
            # Nếu vẫn không lấy được level_id, bỏ qua event này
            if not level_id:
                continue

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

        # Gọi API AppMetrica
        url = "https://api.appmetrica.yandex.com/logs/v1/export/events.json"
        params = {
            "application_id": app_info['app_id'],
            "date_since": str(job['date_since']), 
            "date_until": str(job['date_until']),
            "fields": "event_name,event_timestamp,event_json",
            "limit": 1000000 
        }
        headers = {"Authorization": f"OAuth {app_info['api_token']}"}

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

# ==========================================
# PHẦN 4: LOGIC CHẠY TAY (MANUAL) - [FIXED RETRY LOGIC]
# ==========================================
def perform_manual_etl(app_id, run_type='manual', is_demo=False, retry_job_id=None):
    """
    Hàm xử lý chạy tay (Manual), Demo và RETRY CHUẨN XÁC.
    """
    if is_system_busy():
        print(f"❌ System BUSY. Skip run for App {app_id}.")
        return

    set_system_busy(True, app_id, run_type)
    hist_id = None

    try:
        conn = get_db()
        if not conn: return
        
        # 1. TẠO HISTORY RECORD
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
        
        # [MỚI] TẠO CỜ DỪNG CHO JOB NÀY
        stop_event = threading.Event()
        JOB_STOP_EVENTS[hist_id] = stop_event

        # Hàm log helper
        def log(msg):
            print(msg)
            append_log_to_db(hist_id, msg)

        # 2. LẤY CẤU HÌNH APP
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM apps WHERE id=%s", (app_id,))
        app = cur.fetchone()
        
        if not app:
            log("❌ Error: App ID not found.")
            return

        # 3. CẤU HÌNH THỜI GIAN (LOGIC QUAN TRỌNG ĐÃ SỬA)
        date_since = None
        date_until = None
        
        # --- CASE 1: RETRY (Lấy giờ từ Job cũ) ---
        if run_type == 'retry' and retry_job_id:
            cur.execute("SELECT start_time FROM job_history WHERE id = %s", (retry_job_id,))
            old_job = cur.fetchone()
            if old_job and old_job['start_time']:
                target_time = old_job['start_time']
                # Tính lại cửa sổ theo đúng logic cũ (thường là 1 tiếng trước đó)
                # Giả sử chu kỳ chuẩn là 60 phút + 5 phút dư
                fix_to = target_time
                fix_from = target_time - timedelta(minutes=65)
                
                date_until = fix_to.strftime('%Y-%m-%d %H:%M:%S')
                date_since = fix_from.strftime('%Y-%m-%d %H:%M:%S')
                log(f"🔙 RETRY MODE: Lùi thời gian về quá khứ theo Job #{retry_job_id}")
            else:
                log(f"⚠️ Không tìm thấy Job cũ #{retry_job_id}, chuyển sang Manual Mode.")

        # --- CASE 2: DEMO / MANUAL (Nếu không phải Retry hoặc Retry lỗi) ---
        if not date_since:
            now = datetime.now()
            if run_type == 'demo':
                delay_minutes = 45; duration_minutes = 15
                log(f"🧪 DEMO MODE: Target Time = NOW - {delay_minutes}m")
            else:
                delay_minutes = 60; duration_minutes = 30
                log(f"⚙️ MANUAL MODE: Target Time = NOW - {delay_minutes}m")

            end_dt = now - timedelta(minutes=delay_minutes)
            start_dt = end_dt - timedelta(minutes=duration_minutes)
            
            date_since = start_dt.strftime('%Y-%m-%d %H:%M:%S')
            date_until = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        log(f" 🕒 Scanning Window: {date_since} -> {date_until}")
        
        # 4. GỌI API APPMETRICA
        url = "https://api.appmetrica.yandex.com/logs/v1/export/events.json"
        params = {
            "application_id": app['app_id'],
            "date_since": date_since,
            "date_until": date_until,
            "fields": "event_name,event_timestamp,event_json",
            "limit": 1000000
        }
        headers = {"Authorization": f"OAuth {app['api_token']}"}
        
        status = "Failed"
        total_events = 0
        max_retries = 18 # Giảm xuống 18 để tránh spam, vì ta đã tăng thời gian chờ
        
        for i in range(max_retries):
            # [MỚI] KIỂM TRA CỜ DỪNG ĐẦU VÒNG LẶP
            if stop_event.is_set():
                log("🛑 USER STOPPED PROCESS.")
                status = "Cancelled"
                break

            log(f"📡 Requesting AppMetrica (Attempt {i+1}/{max_retries})...")
            resp = requests.get(url, params=params, headers=headers)
            
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                total_events = len(data)
                log(f"✅ Success! Received {total_events} events. Importing...")
                try:
                    # Tách riêng connection cho việc insert để an toàn
                    conn_insert = get_db()
                    cur_insert = conn_insert.cursor()
                    
                    # Dùng Batch Insert (Executemany) cho nhanh và ít lỗi hơn insert từng dòng
                    values = []
                    for d in data:
                        evt_json = json.dumps(d)
                        try: ts = datetime.fromtimestamp(int(d.get('event_timestamp')))
                        except: ts = datetime.now()
                        
                        values.append((
                            app_id, 
                            d.get('event_name', 'unknown'), 
                            evt_json, 
                            1, 
                            ts
                        ))
                    
                    # Thực hiện Insert 1 lần
                    query = """
                        INSERT INTO event_logs (app_id, event_name, event_json, count, created_at) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cur_insert.executemany(query, values)
                    conn_insert.commit()
                    conn_insert.close()
                    
                    # QUAN TRỌNG: Gán status Success sau khi insert thành công
                    status = "Success"
                    log(f"🎉 Done. Imported {total_events} events to DB.")
                    
                except Exception as e_insert:
                    log(f"❌ DB Insert Error: {str(e_insert)}")
                    status = "Failed" # Đánh dấu fail nếu lỗi DB

                break
                
                # Insert vào DB
                conn_insert = get_db()
                cur_insert = conn_insert.cursor()
                for d in data:
                    evt_json = json.dumps(d)
                    try: ts = datetime.fromtimestamp(int(d.get('event_timestamp')))
                    except: ts = datetime.now()
                    cur_insert.execute("INSERT INTO event_logs (app_id, event_name, event_json, count, created_at) VALUES (%s, %s, %s, 1, %s)", 
                                (app_id, d.get('event_name'), evt_json, ts))
                conn_insert.commit()
                conn_insert.close()
                
                status = "Success"
                log(f"🎉 Done. Imported {total_events} events.")
                break
            
            elif resp.status_code == 202:
                # Tăng thời gian chờ lên 3 phút
                log(f"⏳ Server 202 (Preparing Data). Waiting 180s...")
                if stop_event.wait(180): 
                    log("🛑 Stop Signal received while waiting.")
                    status = "Cancelled"
                    break
            
            elif resp.status_code == 429:
                # Xử lý 429 thông minh: Dừng ngay lập tức
                error_text = resp.text
                log(f"⛔ BỊ CHẶN (429)! Server message: {error_text}")
                status = "Failed"
                break # Thoát vòng lặp ngay
            
            else:
                log(f"❌ Error {resp.status_code}: {resp.text}")
                status = "Failed"
                if stop_event.wait(30): break
        else:
            log("❌ Timeout: AppMetrica did not return data after max retries.")
            status = "Failed"
    
    except Exception as e:
        log(f"❌ Critical Error: {str(e)}")
        status = "Failed"
    
    finally:
        # Dọn dẹp cờ
        if hist_id and hist_id in JOB_STOP_EVENTS:
            del JOB_STOP_EVENTS[hist_id]
        set_system_busy(False)
        if hist_id:
            try:
                conn_end = get_db()
                cur_end = conn_end.cursor()
                cur_end.execute("""
                    UPDATE job_history 
                    SET end_time=NOW(), status=%s, total_events=%s 
                    WHERE id=%s
                """, (status, total_events, hist_id))
                conn_end.commit()
                conn_end.close()
            except: pass

# ==========================================
# PHẦN 5: API ENDPOINTS (ĐÃ CẬP NHẬT DASHBOARD)
# ==========================================

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

        # 1. LOAD CONFIG
        config = get_app_config(cur, app_id)
        real_events = config.get('events', {}).get('transaction', {}).get('real_currency', [])
        win_events = config.get('events', {}).get('win', [])
        fail_events = config.get('events', {}).get('fail', [])
        real_currency_symbols = config.get('currency', {}).get('real', ['$'])
        
        # Lấy danh sách Booster Key
        booster_config_list = config.get('boosters', [])
        booster_keys = [b['key'] for b in booster_config_list if 'key' in b]

        currency_patterns = [f"%{s}%" for s in real_currency_symbols]

        # 2. FILTER TIME
        where_clause = "WHERE app_id = %s"; params = [app_id]
        if start_date: where_clause += " AND event_time >= %s"; params.append(start_date + " 00:00:00")
        if end_date: where_clause += " AND event_time <= %s"; params.append(end_date + " 23:59:59")

        # 3. QUERY 1: DOANH THU THẬT (REAL REVENUE)
        cur.execute(f"""
            SELECT COALESCE(SUM(coin_spent), 0)::int as real_revenue
            FROM view_game_stats_cleaned
            {where_clause} AND event_name = ANY(%s) AND (raw_json::text ILIKE ANY(%s))
        """, tuple(params + [real_events, currency_patterns]))
        real_revenue = cur.fetchone()['real_revenue']

        # 4. QUERY 2: TỔNG TIÊU COIN (VIRTUAL SINK) - [ĐOẠN MỚI]
        cur.execute(f"""
            SELECT SUM(
                CASE 
                    WHEN coin_spent > 0 THEN coin_spent 
                    ELSE COALESCE((raw_json->>'coin_price')::int, 0)
                END
            )::int as virtual_sink
            FROM view_game_stats_cleaned
            {where_clause} AND event_type = 'SPEND' AND NOT (event_name = ANY(%s))
        """, tuple(params + [real_events]))
        virtual_sink_data = cur.fetchone()
        virtual_sink = virtual_sink_data['virtual_sink'] if virtual_sink_data and virtual_sink_data['virtual_sink'] else 0

        # 5. METRICS PHỤ
        cur.execute(f"SELECT COUNT(*)::int as total FROM view_game_stats_cleaned {where_clause}", tuple(params))
        total_events = cur.fetchone()['total']
        
        # 6. FAIL RATE & TOTAL PLAYS (FIX V64: STRICT MODE - LỌC SẠCH RÁC)
        # A. Tính Total Plays (Chỉ lấy missionStart của các level thực tế)
        # Khớp với con số 94k
        cur.execute(f"""
            SELECT COUNT(*)::int as count
            FROM view_game_stats_cleaned
            {where_clause} 
            AND level_id != '0' 
            AND event_name = 'missionStart' 
        """, tuple(params))
        total_plays = cur.fetchone()['count'] or 0

        # B. Tính Real Fail (Chỉ lấy missionFail, LOẠI BỎ iapOfferClose)
        # Trước đây ta dùng event_type='FAIL' nên bị dính rác
        cur.execute(f"""
            SELECT COUNT(*)::int as count
            FROM view_game_stats_cleaned
            {where_clause} 
            AND level_id != '0' 
            AND event_name = 'missionFail'
        """, tuple(params))
        real_fail_count = cur.fetchone()['count'] or 0
        
        # C. Tính tỷ lệ chuẩn
        # Fail Rate = Số lần chết / Số lần bắt đầu chơi
        fail_rate = round((real_fail_count / total_plays) * 100, 1) if total_plays > 0 else 0.0

        # 7. CHART MAIN
        cur.execute(f"SELECT event_name as name, COUNT(*)::int as value FROM view_game_stats_cleaned {where_clause} GROUP BY event_name ORDER BY value DESC", tuple(params))
        chart_data = cur.fetchall()

        # 8. BOOSTER REVENUE - [ĐOẠN MỚI QUAN TRỌNG]
        booster_stats = []
        if booster_keys:
            # Bảng giá Hardcode từ kết quả phân tích SQL
            PRICE_MAP = {
                "booster_Add": 60, "booster_Magnet": 80, "booster_Clear": 120,
                "booster_Hammer": 120, "booster_Unlock": 190, "revive_boosterClear": 190
            }

            cur.execute(f"""
                SELECT key as booster_key, SUM(value::numeric)::int as usage_count
                FROM view_game_stats_cleaned
                CROSS JOIN LATERAL jsonb_each_text(raw_json) as kv(key, value)
                {where_clause} AND key = ANY(%s) AND value ~ '^[0-9]+$' 
                GROUP BY key
            """, tuple(params + [booster_keys]))
            
            raw_boosters = cur.fetchall()
            name_map = {b['key']: b['name'] for b in booster_config_list}
            
            for b in raw_boosters:
                key = b['booster_key']
                count = b['usage_count']
                price = PRICE_MAP.get(key, 0)
                
                booster_stats.append({
                    "name": name_map.get(key, key),
                    # Mẹo: Gán doanh thu vào biến value để biểu đồ vẽ theo doanh thu
                    # Hoặc giữ nguyên count tùy bạn. Ở đây tôi để count để vẽ cột, và thêm revenue để hiển thị.
                    "value": count, 
                    "revenue": count * price,
                    "price": price
                })
            
            # Sắp xếp theo doanh thu
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
        print(f"Error dashboard V61: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally: conn.close()

# --- [FIXED] API LẤY LEVEL (NỚI LIMIT LÊN 750) ---
@app.route("/api/levels/<int:app_id>", methods=['GET'])
def get_levels(app_id):
    conn = get_db()
    if not conn: return jsonify([])
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT DISTINCT level_id 
            FROM view_game_stats_cleaned 
            WHERE app_id = %s
              AND level_id ~ '^[0-9]+$'
        """, (app_id,))
        
        rows = cur.fetchall()
        
        levels = []
        for r in rows:
            try:
                lvl_str = r[0]
                lvl_num = int(''.join(filter(str.isdigit, lvl_str)))
                
                # [SỬA LẠI]: Nới trần lên <= 750
                if lvl_num <= 750:
                    levels.append(lvl_num)
            except:
                pass
        
        # Sắp xếp tăng dần
        levels.sort()
        
        return jsonify([str(l) for l in levels])

    except Exception as e:
        print(f"❌ Error getting levels: {e}")
        return jsonify([])
    finally:
        conn.close()

@app.route("/dashboard/<int:app_id>/level-detail", methods=['GET'])
def get_level_detail(app_id):
    level_id = request.args.get('level_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try: page = int(request.args.get('page', 1)); limit = int(request.args.get('limit', 50))
    except: page=1; limit=50
    offset = (page - 1) * limit

    conn = get_db()
    if not conn: return jsonify({"success": False}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Base WHERE
        where_view = "WHERE app_id = %s AND level_id = %s"
        params_view = [app_id, str(level_id)]
        where_analytics = "WHERE app_id = %s AND level_name = %s"
        params_analytics = [app_id, f"Level {level_id}"] 

        if start_date:
            where_view += " AND event_time >= %s"
            params_view.append(start_date + " 00:00:00")
            where_analytics += " AND created_at >= %s"
            params_analytics.append(start_date + " 00:00:00")
        if end_date:
            where_view += " AND event_time <= %s"
            params_view.append(end_date + " 23:59:59")
            where_analytics += " AND created_at <= %s"
            params_analytics.append(end_date + " 23:59:59")

        # --- METRICS (Giữ nguyên) ---
        cur.execute(f"""
            SELECT event_type, COUNT(*)::int as count, COALESCE(SUM(coin_spent), 0)::int as revenue
            FROM view_game_stats_cleaned
            {where_view} AND event_type IN ('START', 'FAIL', 'WIN', 'SPEND')
            AND event_name != 'iapOfferClose'
            GROUP BY event_type
        """, tuple(params_view))
        rows = cur.fetchall()
        data_map = {r['event_type']: r for r in rows}
        count_start = data_map.get('START', {}).get('count', 0)
        count_win = data_map.get('WIN', {}).get('count', 0)
        count_fail = data_map.get('FAIL', {}).get('count', 0)
        count_spend = data_map.get('SPEND', {}).get('count', 0)
        rev_spend = data_map.get('SPEND', {}).get('revenue', 0)
        real_plays = count_win + count_fail
        adjusted_start = max(count_start, real_plays)
        win_rate = round((count_win / real_plays) * 100, 1) if real_plays > 0 else 0

        # 1. Định nghĩa Bảng Giá & Bảng Tên (Hardcode chuẩn)
        PRICE_MAP = {
            "Hammer": 120, "Magnet": 80, "Add": 60, 
            "Unlock": 190, "Clear": 120, "Revive Clear": 190
        }

        DISPLAY_MAP = { 
            "Add": "Add Moves ➕", 
            "Hammer": "Hammer 🔨", 
            "Magnet": "Magnet 🧲", 
            "Unlock": "Unlock 🔓", 
            "Clear": "Clear 🧹", 
            "Revive Clear": "Revive 💖" 
        }
        
        # 2. Query Database (Chỉ chạy 1 lần duy nhất)
        cur.execute(f"""
            SELECT TRIM(INITCAP(REPLACE(REPLACE(key, 'booster', ''), '_', ' '))) as item_name, SUM(value::numeric)::int as usage_count
            FROM view_game_stats_cleaned
            CROSS JOIN LATERAL jsonb_each_text(raw_json) as kv(key, value)
            {where_view} AND event_type IN ('WIN', 'FAIL')
            AND key ILIKE '%%booster%%' AND value ~ '^[0-9]+$' AND value::numeric > 0
            GROUP BY key
        """, tuple(params_view))
        
        # 3. Xử lý dữ liệu
        booster_usage_list = []
        for row in cur.fetchall():
            raw_name = row['item_name'] # Tên gốc (VD: Magnet)
            count = row['usage_count']
            
            # A. Lấy tên đẹp có Icon (Nếu không có trong map thì dùng tên gốc)
            display_name = DISPLAY_MAP.get(raw_name, raw_name)
            
            # B. Lấy giá tiền (Dùng tên gốc để so khớp)
            unit_price = 0
            if "Hammer" in raw_name: unit_price = PRICE_MAP["Hammer"]
            elif "Magnet" in raw_name: unit_price = PRICE_MAP["Magnet"]
            elif "Add" in raw_name: unit_price = PRICE_MAP["Add"]
            elif "Unlock" in raw_name: unit_price = PRICE_MAP["Unlock"]
            elif "Clear" in raw_name and "Revive" not in raw_name: unit_price = PRICE_MAP["Clear"]
            elif "Revive" in raw_name: unit_price = PRICE_MAP["Revive Clear"]
            
            booster_usage_list.append({
                "item_name": display_name, # <--- Đã có icon!
                "usage_count": count,
                "revenue": count * unit_price, 
                "price": unit_price,
                "type": "Used"
            })

        # 4. Sắp xếp & Tính tổng (Cho KPI ARPU)
        booster_usage_list.sort(key=lambda x: x['revenue'], reverse=True)
        top_item_name = booster_usage_list[0]['item_name'] if booster_usage_list else "None"
        total_level_revenue = sum(item['revenue'] for item in booster_usage_list)

        cur.execute(f"""SELECT ROUND(AVG(coin_balance) FILTER (WHERE event_type='WIN'), 0)::int as avg_balance_win FROM view_game_stats_cleaned {where_view} AND event_type IN ('WIN', 'FAIL')""", tuple(params_view))
        wallet_stats = cur.fetchone()
        top_item_name = booster_usage_list[0]['item_name'] if booster_usage_list and booster_usage_list[0]['usage_count'] > 0 else "None"
        metrics = { 
            "total_plays": real_plays, 
            "win_rate": win_rate, 
            "arpu": total_level_revenue,  # <--- ĐIỂM SỬA QUAN TRỌNG
            "avg_balance": wallet_stats.get('avg_balance_win', 0) if wallet_stats else 0, 
            "top_item": top_item_name 
        }
        funnel_data = [ {"event_type": "START", "count": adjusted_start, "revenue": 0}, {"event_type": "WIN", "count": count_win, "revenue": 0}, {"event_type": "SPEND", "count": count_spend, "revenue": rev_spend}, {"event_type": "FAIL", "count": count_fail, "revenue": 0} ]
        
        cur.execute(f"""SELECT status, COALESCE(SUM(total_cost), 0)::int as total_cost FROM level_analytics {where_analytics} GROUP BY status""", tuple(params_analytics))
        cost_rows = cur.fetchall()
        cost_distribution = []
        for r in cost_rows:
            if r['total_cost'] > 0: cost_distribution.append({ "name": "Cost to Win" if r['status'] == 'WIN' else "Wasted on Fail", "value": r['total_cost'] })
        
        # 1. Đếm tổng số dòng để phân trang
        cur.execute(f"SELECT COUNT(*)::int as total FROM view_game_stats_cleaned {where_view}", tuple(params_view))
        total_logs = cur.fetchone()['total']
        total_pages = (total_logs + limit - 1) // limit

        # 2. Lấy dữ liệu (Bắt buộc lấy raw_json)
        cur.execute(f"""
            SELECT 
                to_char(event_time, 'HH24:MI:SS DD/MM') as time, 
                user_id, event_name, coin_spent, 
                raw_json 
            FROM view_game_stats_cleaned {where_view}
            ORDER BY event_time DESC LIMIT %s OFFSET %s
        """, tuple(params_view + [limit, offset]))
        
        raw_logs = cur.fetchall()
        processed_logs = []

        for row in raw_logs:
            # A. ÉP KIỂU JSON AN TOÀN (Handle String vs Dict)
            raw_data = row['raw_json']
            data = {}
            
            if isinstance(raw_data, str):
                try: data = json.loads(raw_data)
                except: data = {} 
            elif isinstance(raw_data, dict):
                data = raw_data
            if isinstance(data, list) and len(data) > 0: data = data[0]
            if not isinstance(data, dict): data = {}
            data_lower = {k.lower(): v for k, v in data.items()}

            # B. XỬ LÝ USER (CONTEXT)
            user_display = "Guest"
            if 'levelID' in data and str(data['levelID']).isdigit(): user_display = f"Guest (Lv.{data['levelID']})"
            elif 'dayChallenge' in data: user_display = f"Guest (Daily #{data['dayChallenge']})"
            elif 'packID' in data: user_display = f"Guest (Shop)"
            
            # C. XỬ LÝ DETAIL (XÂY DỰNG CHUỖI)
            details = []
            
            # - Shop / Pack
            if 'packid' in data_lower: 
                curr = data_lower.get('currencycode', '')
                amt = data_lower.get('amount', '')
                details.append(f"📦 Pack: {data_lower['packid']} ({amt} {curr})")
            
            # - Progress (Thêm emoji cờ)
            if 'level_progresspercent' in data_lower: 
                details.append(f"🚩 Progress: {data_lower['level_progresspercent']}")
            
            # - Win/Fail Logic (Thêm tổng số object và dấu X)
            if 'objectunsolve' in data_lower: 
                total = data_lower.get('objecttotal', '?')
                details.append(f"❌ Unsolved: {data_lower['objectunsolve']}/{total}")
            
            # - Price (Thêm emoji tag giá)
            price = data_lower.get('coin_price') or data_lower.get('coinprice') or data_lower.get('cost')
            if price is not None:
                try:
                    p_val = int(price)
                    if p_val > 0: details.append(f"🏷️ Price: {p_val}")
                    else: details.append("🏷️ Free")
                except: pass

            # - Balance (Thêm emoji túi tiền)
            bal = data_lower.get('coinbalance') or data_lower.get('coin_balance')
            if bal: details.append(f"💰 Wallet: {bal}")
            
            # - Time Play (Mới thêm vào - Quan trọng để biết chơi lâu hay mau)
            t_play = data_lower.get('timeplay') or data_lower.get('time_play')
            if t_play: details.append(f"⏱️ Time: {t_play}s")

            # - Boosters (Thêm emoji tia sét)
            used_boosters = []
            for k, v in data.items():
                if 'booster' in k.lower() and str(v).isdigit() and int(v) > 0:
                    clean_name = k.replace('booster_', '').replace('booster','').capitalize()
                    used_boosters.append(f"{clean_name} x{v}")
            if used_boosters: details.append(f"⚡ Used: {', '.join(used_boosters)}")

            # D. CHỐT CHUỖI DETAIL (FALLBACK MODE)
            detail_str = " | ".join(details)
            if not detail_str: detail_str = "-"
            
            # E. TẠO OBJECT TRẢ VỀ (BẮN ĐA ĐIỂM VÀO MỌI BIẾN)
            processed_logs.append({
                "time": row['time'],
                "user_id": user_display,
                "event_name": row['event_name'],
                "coin_spent": row['coin_spent'],
                "item_name": detail_str 
            })

        return jsonify({
            "success": True,
            "metrics": metrics, "funnel": funnel_data, "booster_usage": booster_usage_list, "cost_distribution": cost_distribution,
            "logs": { 
                "data": processed_logs, 
                "pagination": { "current": page, "total_pages": total_pages, "total_records": total_logs } 
            }
        })

    except Exception as e:
        print(f"Level Detail Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally: conn.close()

@app.route("/dashboard/<int:app_id>/strategic", methods=['GET'])
def get_strategic_overview(app_id):
    conn = get_db()
    if not conn: return jsonify({"success": False, "error": "DB Connection failed"}), 500

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        config = get_app_config(cur, app_id)
        real_events = config.get('events', {}).get('transaction', {}).get('real_currency', [])
        
        where_clause = "WHERE app_id = %s AND level_id IS NOT NULL"
        params = [app_id]
        if start_date:
            where_clause += " AND event_time >= %s"
            params.append(start_date + " 00:00:00")
        if end_date:
            where_clause += " AND event_time <= %s"
            params.append(end_date + " 23:59:59")

        # QUERY V70: Đưa điều kiện lọc vào trong FILTER để an toàn hơn
        cur.execute(f"""
            SELECT 
                level_id,
                COUNT(*) FILTER (WHERE event_type = 'START')::int as total_plays,
                -- Chỉ đếm Fail nếu KHÔNG PHẢI là iapOfferClose
                COUNT(*) FILTER (WHERE event_type = 'FAIL' AND (event_name != 'iapOfferClose' OR event_name IS NULL))::int as fail_count,
                COALESCE(SUM(coin_spent) FILTER (WHERE event_name = ANY(%s)), 0)::float as revenue
            FROM view_game_stats_cleaned
            {where_clause}
            GROUP BY level_id
        """, (real_events,) + tuple(params))
        
        rows = cur.fetchall()
        
        balance_chart = []
        for r in rows:
            lvl_str = str(r['level_id'])
            digits = ''.join(filter(str.isdigit, lvl_str))
            
            if digits:
                lvl_num = int(digits)
                if lvl_num <= 750: 
                    plays = r['total_plays']
                    fails = r['fail_count']
                    rev = r['revenue']
                    
                    if plays > 0:
                        fail_rate = round((fails / plays) * 100, 1)
                        if fail_rate > 100: fail_rate = 100.0
                    else:
                        fail_rate = 0.0
                    
                    display_name = f"Lv.{lvl_num}"
                    if lvl_num == 0: display_name = "Lobby/Tut"

                    if plays > 0 or rev > 0:
                        balance_chart.append({
                            "name": display_name,
                            "level_index": lvl_num,
                            "revenue": rev,
                            "fail_rate": fail_rate,
                            "plays": plays
                        })

        balance_chart.sort(key=lambda x: x['level_index'])

        return jsonify({
            "success": True,
            "balance_chart": balance_chart
        })

    except Exception as e:
        print(f"Strategic Error: {e}")
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