from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class EtlLogResponse(BaseModel):
    id: int
    run_time: datetime
    status: str
    records_processed: int
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True
