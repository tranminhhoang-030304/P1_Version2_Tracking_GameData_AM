# Quick Integration Testing Guide

## 1️⃣ Start Backend (FastAPI)

```bash
# Terminal 1: Backend
cd c:\Users\Admin\OneDrive\Máy tính\game-analytics-dashboard

# Install if needed
pip install -r requirements.txt

# Run FastAPI
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

Expected output:
```
INFO:     Uvicorn running on http://127.0.0.1:8000
INFO:     Application startup complete
```

## 2️⃣ Seed Database (Optional but Recommended)

```bash
# Terminal 2: Same project directory
python seed_data.py
```

Expected output:
```
Seeding complete: items, transactions, etl_logs
```

This creates:
- 200 items across 20 levels
- 2000 transactions
- 3600 ETL log entries

## 3️⃣ Start Frontend (Next.js)

```bash
# Terminal 3: Same project directory
npm run dev
# or if using pnpm
pnpm dev
```

Expected output:
```
▲ Next.js 14.x
  - Local:        http://localhost:3000
```

## 4️⃣ Test Dashboard

1. Open browser: http://localhost:3000
2. You should see:
   - Dashboard page with sidebar
   - **Loading skeletons** while fetching data
   - **Total Revenue** stat card with real data from `/api/analytics/revenue`
   - **Avg Fail Rate** stat card with real data from `/api/analytics/fail-rate`
   - **Revenue vs Fail Rate Chart** with data from `/api/analytics/items-by-level`

## 5️⃣ Verify API Endpoints

Test each endpoint directly:

```bash
# Test 1: Health check
curl http://127.0.0.1:8000/health

# Test 2: Revenue data (last 30 days)
curl http://127.0.0.1:8000/api/analytics/revenue

# Test 3: Fail rate data
curl http://127.0.0.1:8000/api/analytics/fail-rate

# Test 4: Items by level
curl http://127.0.0.1:8000/api/analytics/items-by-level

# Test 5: Items detail for level 5
curl http://127.0.0.1:8000/api/analytics/items-by-level/5
```

## 6️⃣ Test Error Handling

1. **Stop the backend** (Ctrl+C in terminal 1)
2. **Refresh dashboard** in browser
3. You should see:
   - **Error alerts** appearing on components
   - **Fallback data** displayed (mock data with 10 levels)
   - **UI does NOT crash**
4. **Restart backend** (run command from step 1 again)
5. **Refresh dashboard** - should load real data again

## 7️⃣ Browser DevTools Inspection

1. Open DevTools (F12)
2. Go to **Network tab**
3. Refresh page
4. Look for requests to `/api/analytics/*`
5. Verify:
   - Status: **200 OK**
   - Response format matches schema
   - Response time < 100ms

## Common Response Examples

### /api/analytics/revenue
```json
[
  {
    "date": "2024-01-01",
    "revenue": 1234.56,
    "transactions": 42
  },
  {
    "date": "2024-01-02", 
    "revenue": 2345.67,
    "transactions": 58
  }
]
```

### /api/analytics/items-by-level
```json
[
  {
    "level": 1,
    "count": 50,
    "total_revenue": 500.00
  },
  {
    "level": 2,
    "count": 75,
    "total_revenue": 1250.00
  }
]
```

## Troubleshooting Checklist

- [ ] Backend running on http://127.0.0.1:8000
- [ ] Database file exists or PostgreSQL configured
- [ ] Seed data loaded (check with `curl http://127.0.0.1:8000/api/analytics/revenue`)
- [ ] Frontend running on http://localhost:3000
- [ ] API proxy configured in next.config.mjs
- [ ] Browser console shows no CORS errors
- [ ] Dashboard loading skeletons appear (good sign!)
- [ ] Stats cards populate with real numbers
- [ ] Chart renders with data
- [ ] Error handling works when backend stopped

## Files Modified in This Session

✅ `backend/services/analytics_service.py` - Rebuilt with all methods
✅ `backend/models/item.py` - Fixed import path
✅ `seed_data.py` - Fixed Item import
✅ Created `INTEGRATION_STATUS.md` - Comprehensive status

## Environment Variables (Optional)

To use PostgreSQL instead of SQLite:

```bash
# PowerShell
$env:POSTGRES_URL = "postgresql://user:password@localhost/game_analytics"
python -m uvicorn backend.main:app --reload

# Or add to .env file (if using python-dotenv)
POSTGRES_URL=postgresql://user:password@localhost/game_analytics
```

Without setting POSTGRES_URL, defaults to SQLite: `sqlite:///./game_data.db`

---

**Status**: Integration complete and ready for testing
**Time to setup**: ~2 minutes (backend + frontend + seeding)
**Expected result**: Fully functional analytics dashboard with real API data
