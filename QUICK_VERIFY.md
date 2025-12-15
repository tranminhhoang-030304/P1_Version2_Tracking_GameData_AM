# ✅ Integration Checklist - Quick Reference

## 🎯 Pre-Flight Checks

- [ ] Backend chạy tại `http://127.0.0.1:8000`
- [ ] Frontend chạy tại `http://localhost:3001`
- [ ] Database có dữ liệu seed (chạy `python seed_data.py` nếu chưa)

## 🔌 Backend Verification

```bash
# Terminal riêng - Test backend API
python test_backend.py
```

Expected output:
```
✅ PASS: Health
✅ PASS: Revenue
✅ PASS: Items by Level
✅ PASS: Items Detail
✅ PASS: Fail Rate

✅ All tests passed! Integration ready.
```

## 📱 Frontend Verification

### Open Dashboard
1. Trình duyệt: http://localhost:3001
2. Xem trang Dashboard

### Check Each Component (theo thứ tự)

#### 1️⃣ DashboardStats (Stats Cards - Top)
- [ ] "Total Revenue" card hiển thị số lớn (không 0)
- [ ] "Total Items" card hiển thị "200+"
- [ ] "Avg Fail Rate" card hiển thị % (không 0%)
- [ ] Loading skeleton hiệu hiện khi tải
- [ ] Không thấy error alert (nếu backend chạy đúng)

✅ **Expected:** 3 cards với dữ liệu thực từ API

#### 2️⃣ RevenueChart (Main Chart - Middle)
- [ ] Biểu đồ không trống
- [ ] Có 20 bars (Level 1 → Level 20)
- [ ] X-axis: Level 1, 2, 3...
- [ ] Y-axis Left: $12k, $24k, $36k (Revenue)
- [ ] Y-axis Right: 0%, 20%, 40% (Fail Rate)
- [ ] Có line chạy qua trên bars (Fail Rate line)
- [ ] Loading skeleton hiệu hiện khi tải

✅ **Expected:** Composed chart với data thực, không fallback data

#### 3️⃣ BoostersTable (Top Used Boosters - Bottom Left)
- [ ] Hiển thị 5 hàng (Rank 1-5)
- [ ] ❌ **Không phải** Double XP, Extra Life, Score Multiplier...
- [ ] ✅ **Phải là** Level 1, Level 2, Level 3... (hoặc item names từ DB)
- [ ] Cột "Usage" hiển thị số lớn (count từ database)
- [ ] Cột "Trend" hiển thị + hoặc - (có màu)
- [ ] Loading skeleton hiệu hiện khi tải

✅ **Expected:** Table với top 5 levels by revenue từ database

#### 4️⃣ DrilldownSection (Item Usage by Level - Bottom Right)
- [ ] Dropdown "Select level" có dữ liệu (Level 1, 2, 3...)
- [ ] Default level là một level ở giữa (không luôn là Level 5)
- [ ] Pie chart hiển thị top items cho level được chọn
- [ ] Khi select level khác → pie chart update
- [ ] Loading skeleton hiệu hiện khi tải level mới
- [ ] Pie chart có 5 màu khác nhau (cyan, magenta, green, orange, blue)

✅ **Expected:** Pie chart với items thực từ API, không fallback data

## 🌐 Network Inspector Check

1. Mở DevTools: **F12**
2. Tab **Network**
3. Refresh trang: **Ctrl+R**
4. Filter: Type "api" hoặc search "/api"

Verify:
- [ ] Request `/api/analytics/revenue` → **Status 200** ✅
- [ ] Request `/api/analytics/fail-rate` → **Status 200** ✅
- [ ] Request `/api/analytics/items-by-level` → **Status 200** ✅
- [ ] Response time < 100ms (mỗi request)
- [ ] Response không trống (array có dữ liệu)

## 🚀 If Everything is ✅

**Congratulations!** Integration is working perfectly.

Next steps:
1. Customize styling nếu muốn
2. Thêm thêm endpoints nếu cần tính năng mới
3. Deploy lên production

## 🔧 If Something is ❌

Follow troubleshooting in `INTEGRATION_GUIDE_VI.md`:
1. Kiểm tra Backend Health
2. Verify Data seed
3. Check Proxy config
4. Xem Console errors (F12 → Console tab)

## 📋 Common Issues Quick Fix

| Issue | Quick Fix |
|-------|-----------|
| Biểu đồ trống | `python seed_data.py` → Refresh |
| "Failed to fetch" | Kiểm tra backend port 8000 chạy không |
| Dữ liệu là fallback | Kiểm tra DevTools Console → error gì |
| Dropdown trống | Backend endpoint không trả dữ liệu |

## ⏱️ Time Estimate

- ✅ Backend setup: 2 mins
- ✅ Frontend start: 1 min
- ✅ Seed database: 2 mins
- ✅ Verification: 3 mins

**Total: ~8 minutes** từ 0 → fully working dashboard

---

✅ **Integration Status**: COMPLETE - Ready to verify!
