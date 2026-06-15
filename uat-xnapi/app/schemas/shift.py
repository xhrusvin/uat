from typing import Any, List, Literal, Optional
from pydantic import BaseModel

CRITERIA = Literal[
    "User Type",
    "Automation Status",
    "County",
    "Client",
]

class ShiftListRequest(BaseModel):
    search:     str = ""
    criteria:   Optional[CRITERIA] = None   # narrows the search field
    page:       int = 1
    per_page:   int = 20
    start_date: Optional[str] = None
    end_date:   Optional[str] = None
    sort_by:    str = "date"
    sort_order: str = "desc"


class ShiftListResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    message: Optional[str] = None
    total: Optional[int] = None
    page: Optional[int] = None
    per_page: Optional[int] = None
