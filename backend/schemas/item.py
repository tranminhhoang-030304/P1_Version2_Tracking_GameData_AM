from pydantic import BaseModel
from typing import Optional

# 1. Class cơ sở (Base)
class ItemBase(BaseModel):
    booster_key: str
    booster_name: str
    coin_cost: int 

# 2. Dữ liệu khi TẠO MỚI (Create) - Kế thừa từ Base
class ItemCreate(ItemBase):
    pass

# 3. Dữ liệu khi CẬP NHẬT (Update) - Các trường có thể để trống (Optional)
# Class này quan trọng để tránh lỗi ImportError ở Router
class ItemUpdate(BaseModel):
    booster_name: Optional[str] = None
    coin_cost: Optional[int] = None

# 4. Dữ liệu khi TRẢ VỀ (Response) - Có thêm ID
class ItemResponse(ItemBase):
    id: int

    class Config:
        from_attributes = True