# Game Analytics Dashboard - Integration Guide

## Status: ✅ Complete

Frontend-Backend integration is complete with the following setup:

### Backend (FastAPI)
- **Entry point:** `api/index.py` (for Vercel)
- **Main app:** `app/main.py` (FastAPI initialization with all routers)
- **Framework:** FastAPI with SQLAlchemy 2.0 ORM
- **Database:** PostgreSQL (configured via `POSTGRES_URL` env variable)
- **Port:** `http://127.0.0.1:8000`

### Frontend (Next.js)
- **Configuration:** `next.config.mjs` with rewrites to backend
- **API Prefix:** `/api/` routes automatically proxied to `http://127.0.0.1:8000/api/`
- **Components:** Connected to API with error handling and fallback data
- **State Management:** React hooks with fetch API

---

## Quick Start (Local Development)

### Prerequisites
- Python 3.9+
- Node.js 16+
- PostgreSQL (or Vercel Postgres connection string)

### Backend Setup

1. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

2. **Configure environment variables (.env):**
```env
POSTGRES_URL=postgresql://user:password@localhost:5432/game_analytics
DEBUG=True
API_PREFIX=/api
```

For Vercel Postgres, get the connection string from Vercel dashboard and use:
```env
POSTGRES_URL=<your-vercel-postgres-url>
```

3. **Create database tables and seed data:**
```bash
python seed_data.py
```

This will create:
- 200 items (10 per level, across 20 levels)
- 2000 transaction records
- 3600 ETL log entries (one per hour, simulating historical data)

4. **Run FastAPI server:**
```bash
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Server will start at: `http://127.0.0.1:8000`
API Documentation: `http://127.0.0.1:8000/docs`

### Frontend Setup

1. **Install Node dependencies:**
```bash
npm install
# or
pnpm install
```

2. **Run Next.js dev server:**
```bash
npm run dev
# or
pnpm dev
```

Frontend will start at: `http://localhost:3000`

---

## API Routes (All available)

### Items (CRUD)
- `GET /api/items` - List all items (paginated)
- `GET /api/items/{id}` - Get specific item
- `POST /api/items` - Create new item
- `PUT /api/items/{id}` - Update item
- `DELETE /api/items/{id}` - Delete item

### Analytics
- `GET /api/analytics/revenue` - Revenue by date
- `GET /api/analytics/fail-rate` - Fail rate by date
- `GET /api/analytics/items-by-level` - Item distribution by level
- `GET /api/analytics/items-by-level/{level}` - Detailed items for specific level

### ETL Monitoring
- `GET /api/etl/logs` - List ETL logs
- `GET /api/etl/logs/{id}` - Get specific log
- `POST /api/etl/run` - Trigger ETL process

### Health Check
- `GET /api/health` - API health status

---

## Frontend Components Integration

### Updated Components (API-Connected)

**1. DashboardStats Component** (`components/dashboard-stats.tsx`)
- Fetches real revenue and fail rate data
- Displays with loading state and error handling
- Falls back gracefully if backend unavailable

**2. RevenueChart Component** (`components/revenue-chart.tsx`)
- Fetches items by level and fail rate analytics
- Displays in composed chart (Bar + Line)
- Error alert with fallback mock data

**3. DrilldownSection Component** (`components/drilldown-section.tsx`)
- Dynamically loads items by selected level
- Interactive pie chart with real data
- Error handling with fallback visualization

**4. BoostersCRUDTable Component** (`components/boosters-crud-table.tsx`)
- Full CRUD operations on items
- Fetches from `/api/items`
- Create, Read, Update, Delete functionality
- Error handling for all operations

---

## Error Handling

All components include:
- **Loading state** - Shows skeleton loaders while fetching
- **Error alerts** - Displays user-friendly error messages
- **Fallback data** - Gracefully falls back to mock data if API unavailable
- **API Error Component** - `ApiErrorAlert` component for consistent error UI

Example error handling:
```typescript
const [error, setError] = useState<string | null>(null)

try {
  const res = await fetch('/api/endpoint')
  if (!res.ok) throw new Error('API failed')
} catch (err) {
  setError(err.message)
}

// Render
{error && <ApiErrorAlert message={error} />}
```

---

## Next.js Configuration

**File:** `next.config.mjs`

Rewrites configuration:
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

This allows frontend to call `/api/...` without CORS issues during local development.

---

## Deployment to Vercel

### Backend Deployment

1. **Push code to GitHub repository**

2. **Create Vercel project from GitHub**
   - Select your repository
   - Vercel auto-detects Python project

3. **Set environment variables in Vercel Dashboard:**
   - Project Settings → Environment Variables
   - Add `POSTGRES_URL` = Your Vercel Postgres connection string

4. **Deploy**
   - Vercel automatically deploys from `api/index.py`

### Frontend Deployment

1. **Update `next.config.mjs` for production:**
```javascript
async rewrites() {
  return {
    beforeFiles: [
      {
        source: '/api/:path*',
        destination: 'https://<your-vercel-backend-url>/api/:path*',
      },
    ],
  }
}
```

2. **Deploy to Vercel**
   - Connect repository or push updates
   - Vercel auto-deploys Next.js

---

## Testing Endpoints

### Health Check
```bash
curl http://127.0.0.1:8000/api/health
```

### Get Items
```bash
curl http://127.0.0.1:8000/api/items?limit=5
```

### Get Analytics
```bash
curl http://127.0.0.1:8000/api/analytics/items-by-level
curl http://127.0.0.1:8000/api/analytics/revenue
```

### Get Items by Level (for drill-down)
```bash
curl http://127.0.0.1:8000/api/analytics/items-by-level/1
```

---

## File Structure

```
game-analytics-dashboard/
├── api/
│   └── index.py                   # Vercel entry point
├── app/
│   ├── main.py                    # FastAPI app with routers
│   ├── config.py                  # Configuration (POSTGRES_URL)
│   ├── database.py                # SQLAlchemy setup
│   ├── models/                    # Database models
│   ├── schemas/                   # Pydantic schemas
│   ├── routers/                   # API endpoints
│   ├── services/                  # Business logic
│   └── utils/                     # Helpers
├── components/
│   ├── dashboard-stats.tsx        # ✅ NEW - Stats component with API
│   ├── api-error-alert.tsx        # ✅ NEW - Error display
│   ├── revenue-chart.tsx          # ✅ UPDATED - API integration
│   ├── drilldown-section.tsx      # ✅ UPDATED - API integration
│   ├── boosters-crud-table.tsx    # ✅ UPDATED - Full CRUD
│   └── ...
├── hooks/
│   └── use-api.ts                 # ✅ UPDATED - API hook with error handling
├── app/
│   └── page.tsx                   # ✅ UPDATED - Uses DashboardStats
├── next.config.mjs                # ✅ UPDATED - Rewrites config
├── requirements.txt               # ✅ Backend dependencies
├── vercel.json                    # ✅ Vercel config
├── seed_data.py                   # ✅ Seed script
└── ...
```

---

## Common Issues & Solutions

### Issue: CORS Error
**Solution:** Check `next.config.mjs` has rewrites configured correctly. Both dev servers must be running.

### Issue: Backend connection refused
**Solution:** Ensure FastAPI is running on `http://127.0.0.1:8000`. Check `POSTGRES_URL` env variable.

### Issue: No data appears
**Solution:** Run `python seed_data.py` to populate database with test data.

### Issue: API returns 500 error
**Solution:** Check FastAPI logs for database connection issues. Verify `POSTGRES_URL` is correct.

---

## Performance Notes

- **Connection pooling:** SQLAlchemy configured with `pool_pre_ping=True` for serverless
- **Batch operations:** Seed script uses bulk_save for efficiency
- **Pagination:** Items endpoint supports `skip` and `limit` parameters
- **Caching:** Frontend components use React state (no external cache needed for MVP)

---

## Next Steps

1. ✅ Run backend locally: `uvicorn app.main:app --reload`
2. ✅ Run frontend locally: `npm run dev`
3. ✅ Test endpoints at `http://localhost:3000`
4. ✅ Verify data appears in dashboard
5. ✅ Deploy to Vercel when ready

---

Generated: December 12, 2025
