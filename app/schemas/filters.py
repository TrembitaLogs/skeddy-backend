import re

from pydantic import BaseModel, Field, field_validator

_VALID_DAYS = {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}
_START_TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
_DEFAULT_WORKING_DAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


class FiltersResponse(BaseModel):
    """Response schema for GET /filters."""

    min_price: float = 20.0
    start_time: str = "06:30"
    working_time: int = 24
    working_days: list[str] = _DEFAULT_WORKING_DAYS


class FiltersUpdateRequest(BaseModel):
    """Request schema for PUT /filters."""

    min_price: float = Field(ge=10.0, le=100000)
    start_time: str
    working_time: int = Field(ge=1, le=24)
    working_days: list[str]

    @field_validator("start_time")
    @classmethod
    def validate_start_time(cls, v: str) -> str:
        if not _START_TIME_RE.match(v):
            raise ValueError("start_time must be in HH:MM 24h format")
        return v

    @field_validator("working_days")
    @classmethod
    def validate_working_days(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one working day is required")
        if not set(v).issubset(_VALID_DAYS):
            invalid = set(v) - _VALID_DAYS
            raise ValueError(f"Invalid working days: {invalid}")
        return v
