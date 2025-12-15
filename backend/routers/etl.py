from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from backend.database import get_db
from backend.schemas.etl_log import EtlLogResponse
from backend.services.etl_service import EtlService

router = APIRouter()

@router.get("/logs", response_model=List[EtlLogResponse])
async def get_etl_logs(
    skip: int = 0,
    limit: int = 20,
    status: str = None,
    db: Session = Depends(get_db)
):
    service = EtlService(db)
    return service.get_logs(skip=skip, limit=limit, status=status)

@router.get("/logs/{log_id}", response_model=EtlLogResponse)
async def get_etl_log(log_id: int, db: Session = Depends(get_db)):
    service = EtlService(db)
    log = service.get_log_by_id(log_id)
    if not log:
        raise HTTPException(status_code=404, detail="ETL log not found")
    return log

@router.post("/run")
async def trigger_etl(db: Session = Depends(get_db)):
    service = EtlService(db)
    log = await service.run_etl_process()
    return {"message": "ETL process started", "log_id": log.id}
