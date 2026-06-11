from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class ShiftFilters(BaseModel):
    location: Optional[List[str]] = None


class ShiftListRequest(BaseModel):
    search: str = ""
    page: int = 1
    per_page: int = 20
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    sort_by: str = "date"
    sort_order: str = "desc"
    filters: Optional[ShiftFilters] = None


class ShiftListResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    message: Optional[str] = None
    total: Optional[int] = None
    page: Optional[int] = None
    per_page: Optional[int] = None
