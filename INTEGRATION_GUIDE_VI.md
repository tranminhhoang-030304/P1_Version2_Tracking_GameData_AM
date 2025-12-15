# 🎮 Game Analytics Dashboard - Integration Guide (Tiếng Việt)

## 📋 Tình trạng hiện tại

- ✅ **Backend (FastAPI)**: Đang chạy tại `http://127.0.0.1:8000` với dữ liệu seed (Búa Thần, Bom Nổ...)
- ✅ **Frontend (Next.js)**: Đang chạy tại `http://localhost:3001`
- ✅ **Proxy**: Được cấu hình trong `next.config.mjs` ✓
- ✅ **Components**: Được cập nhật để fetch dữ liệu thực từ API

## 🔧 Các thay đổi đã thực hiện

### 1. **BoostersTable** (`components/boosters-table.tsx`)
- ❌ Cũ: Dùng dữ liệu mẫu cứng (Double XP, Extra Life...)
- ✅ Mới: Fetch từ `/api/analytics/items-by-level`
- ✅ Hiển thị: Top 5 levels by revenue
- ✅ Loading state: Skeleton loader
- ✅ Error handling: ApiErrorAlert + fallback data

### 2. **RevenueChart** (`components/revenue-chart.tsx`)
- ✅ Fetch từ `/api/analytics/items-by-level`
- ✅ Transform dữ liệu: Level → Revenue vs Fail Rate
- ✅ Loading state: Skeleton loader
- ✅ Error handling: Hiển thị error alert + fallback data

### 3. **DrilldownSection** (`components/drilldown-section.tsx`)
- ✅ Tự động fetch available levels từ `/api/analytics/items-by-level`
- ✅ Fetch detail items cho level được chọn
- ✅ Loading state: Skeleton loader
- ✅ Error handling: Fallback data
- ✅ Dynamic level selection: Không hardcode level 1, 5, 10

### 4. **DashboardStats** (`components/dashboard-stats.tsx`)
- ✅ Sẵn có: Fetch từ `/api/analytics/revenue` và `/api/analytics/fail-rate`

## 🚀 Hướng dẫn chạy

### Terminal 1: Backend FastAPI
```bash
cd c:\Users\Admin\OneDrive\Máy tính\game-analytics-dashboard

# Cài đặt packages (nếu chưa)
pip install -r requirements.txt

# Chạy FastAPI
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

**Output mong đợi:**
```
INFO:     Uvicorn running on http://127.0.0.1:8000
INFO:     Application startup complete
```

### Terminal 2: Seed Database (Optional, nhưng nên làm)
```bash
# Trong cùng project folder
python seed_data.py
```

**Output mong đợi:**
```
Seeding complete: items, transactions, etl_logs
```

### Terminal 3: Frontend Next.js
```bash
# Trong cùng project folder
npm run dev
# hoặc nếu dùng pnpm
pnpm dev
```

**Output mong đợi:**
```
▲ Next.js 14.x
  - Local: http://localhost:3001
```

### Terminal 4: Test Backend (Optional)
```bash
# Trong cùng project folder
python test_backend.py
```

**Output mong đợi:**
```
✅ All tests passed! Integration ready.
```

## ✅ Kiểm tra từng bước

### Bước 1: Verify Backend Health
```bash
curl http://127.0.0.1:8000/health
# Mong đợi: {"status": "healthy"}
```

### Bước 2: Verify Data
```bash
curl http://127.0.0.1:8000/api/analytics/items-by-level | python -m json.tool
# Mong đợi: JSON array với items từ từng level
```

### Bước 3: Mở Dashboard
1. Trình duyệt: http://localhost:3001
2. Xem Dashboard page

### Bước 4: Kiểm tra từng Component
- **DashboardStats**: Phải hiển thị "Total Revenue", "Total Items", "Avg Fail Rate" với số liệu thực
- **RevenueChart**: Biểu đồ không trống, hiển thị Revenue vs Fail Rate by Level
- **BoostersTable**: Hiển thị Top 5 Levels by Revenue (không phải Double XP, Extra Life...)
- **DrilldownSection**: Pie chart hiển thị Top Items cho level được chọn

## 🔍 Browser DevTools Inspection

1. Mở DevTools: **F12**
2. Vào tab **Network**
3. Refresh trang (Ctrl+R hoặc Cmd+R)
4. Tìm các request `/api/analytics/`
5. Kiểm tra:
   - ✅ Status: **200 OK**
   - ✅ Response không trống
   - ✅ Response time < 100ms

**Request mong đợi:**
- `/api/analytics/revenue` - List revenue data
- `/api/analytics/fail-rate` - List fail rate data
- `/api/analytics/items-by-level` - List items grouped by level
- `/api/analytics/items-by-level/5` - Items detail for level 5

## 🐛 Troubleshooting

### Vấn đề: Biểu đồ vẫn trống
**Nguyên nhân:** Dữ liệu chưa được seed hoặc API chưa phản hồi

**Giải pháp:**
```bash
# 1. Chắc chắn backend đang chạy
curl http://127.0.0.1:8000/health

# 2. Seed dữ liệu
python seed_data.py

# 3. Check xem có dữ liệu không
curl http://127.0.0.1:8000/api/analytics/items-by-level

# 4. Refresh frontend
# Mở http://localhost:3001
```

### Vấn đề: "Failed to fetch" error
**Nguyên nhân:** Backend không chạy hoặc CORS error

**Giải pháp:**
1. Kiểm tra backend chạy trên port 8000: `netstat -ano | find "8000"`
2. Kiểm tra next.config.mjs có rewrites: ✅ `destination: 'http://127.0.0.1:8000/api/:path*'`
3. Restart frontend: `Ctrl+C` → `npm run dev`

### Vấn đề: Dữ liệu là mẫu (Fallback Data)
**Nguyên nhân:** API endpoint trả về lỗi

**Giải pháp:**
1. Kiểm tra backend logs (Terminal 1)
2. Test endpoint trực tiếp: `curl http://127.0.0.1:8000/api/analytics/items-by-level`
3. Xem DevTools Console (F12) xem error gì

### Vấn đề: Database connection error
**Nguyên nhân:** SQLite file không có quyền ghi hoặc PostgreSQL chưa setup

**Giải pháp:**
```bash
# Xóa SQLite cũ (nếu có)
del game_data.db

# Chạy lại backend
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000

# Seed dữ liệu mới
python seed_data.py
```

## 📊 API Endpoints Summary

| Endpoint | Method | Purpose | Response |
|----------|--------|---------|----------|
| `/health` | GET | Health check | `{"status": "healthy"}` |
| `/api/analytics/revenue` | GET | Revenue by date | `[{date, revenue, transactions}]` |
| `/api/analytics/fail-rate` | GET | Fail rate by date | `[{date, total_attempts, failed_attempts, fail_rate}]` |
| `/api/analytics/items-by-level` | GET | Items grouped by level | `[{level, count, total_revenue}]` |
| `/api/analytics/items-by-level/{level}` | GET | Items detail for level | `[{item_id, item_name, count, revenue}]` |

## 💡 Proxy Configuration

File `next.config.mjs` đã cấu hình:
```javascript
async rewrites() {
  return {
    beforeFiles: [
      {
        source: '/api/:path*',
        destination: 'http://127.0.0.1:8000/api/:path*',
      },
    ],
  }
}
```

**Ý nghĩa:** Tất cả request `/api/...` từ frontend sẽ tự động forward sang backend.

## 📝 Files Modified

- ✅ `components/boosters-table.tsx` - Now fetches real data from API
- ✅ `components/revenue-chart.tsx` - Improved data transformation logic
- ✅ `components/drilldown-section.tsx` - Dynamic level loading, proper API calls
- ✅ `next.config.mjs` - Proxy already configured
- ➕ `test_backend.py` - New test script

## 🎯 Expected Result

Khi tất cả chạy đúng, bạn sẽ thấy:

✅ **Dashboard Stats**
- Total Revenue: $XXXXX (từ dữ liệu thực)
- Total Items: 200+ (từ dữ liệu thực)
- Avg Fail Rate: XX.X% (từ dữ liệu thực)

✅ **Revenue Chart**
- Biểu đồ Composed (Bar + Line) với 20 levels
- X-axis: Level 1 → Level 20
- Y-axis Left: Revenue ($)
- Y-axis Right: Fail Rate (%)

✅ **Top Used Boosters**
- Hàng 1-5: Top 5 Levels by Revenue
- Không phải Double XP, Extra Life... mà là Level 1, 2, 3...

✅ **Item Usage by Level**
- Dropdown: Select từ Level 1 → Level 20 (tuỳ theo dữ liệu)
- Pie Chart: Top 5 items cho level được chọn

## 🚨 Important Notes

1. **Port Conflicts**: Nếu port 3001 hoặc 8000 đã bị dùng, thay đổi port trong lệnh chạy
2. **Database**: Dùng SQLite mặc định (file `game_data.db`). Nếu xóa file này, chạy lại backend và seed
3. **Performance**: Lần đầu fetch dữ liệu có thể mất vài giây nếu database lớn
4. **Development Mode**: Cả backend (--reload) lẫn frontend đang chạy ở dev mode để dễ debug

---

**Status**: ✅ Integration Complete - Ready for Testing

Ngày: 12/12/2025
