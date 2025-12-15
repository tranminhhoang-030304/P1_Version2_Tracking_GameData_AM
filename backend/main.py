from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.config import settings
from backend.routers import analytics, etl
from backend.database import engine, Base

# Create database tables
Base.metadata.create_all(bind=engine)

# Initialize FastAPI app
app = FastAPI(
    title="Game Data Center API",
    description="API for managing game data, analytics, and ETL processes",
    version="1.0.0"
)

# CORS middleware for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(analytics.router, prefix=f"{settings.API_PREFIX}/analytics", tags=["Analytics"])
app.include_router(etl.router, prefix=f"{settings.API_PREFIX}/etl", tags=["ETL"])


@app.get("/")
async def root():
    return {"message": "Game Data Center API", "status": "running"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
