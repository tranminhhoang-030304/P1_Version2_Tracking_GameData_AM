# debug_etl.py
import sys
import os

# Đảm bảo Python tìm thấy các module
sys.path.append(os.getcwd())

from etl_processor import run_etl_pipeline

print("--- BẮT ĐẦU DEBUG CHẾ ĐỘ TRỰC TIẾP ---")

# Gọi hàm xử lý với App ID = 1 (App của bạn)
try:
    # App ID của bạn là 1 (theo ảnh Log bạn gửi)
    result = run_etl_pipeline(1) 
    
    if result:
        print("\n✅ KẾT QUẢ: THÀNH CÔNG! Hãy kiểm tra DB ngay.")
    else:
        print("\n❌ KẾT QUẢ: THẤT BẠI. Xem thông báo lỗi ở trên.")
        
except Exception as e:
    print(f"\n🔥 LỖI NGHIÊM TRỌNG (CRASH): {e}")
    import traceback
    traceback.print_exc()

print("--- KẾT THÚC DEBUG ---")