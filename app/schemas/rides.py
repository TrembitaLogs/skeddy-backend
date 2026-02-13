import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, field_validator


class RideData(BaseModel):
    """Ride data payload sent by search device (stored as JSONB)."""

    price: float
    pickup_time: str
    pickup_location: str
    dropoff_location: str
    duration: str | None = None
    distance: str | None = None
    rider_name: str | None = None


class CreateRideRequest(BaseModel):
    """Request schema for POST /rides from search device."""

    idempotency_key: str
    event_type: Literal["ACCEPTED"]
    ride_data: RideData

    @field_validator("idempotency_key")
    @classmethod
    def validate_idempotency_key_uuid(cls, v: str) -> str:
        try:
            uuid.UUID(v)
        except ValueError:
            raise ValueError("idempotency_key must be a valid UUID")
        return v


class CreateRideResponse(BaseModel):
    """Response schema for POST /rides."""

    ok: bool = True
    ride_id: uuid.UUID


class RideEventResponse(BaseModel):
    """Single ride event in GET /rides/events response."""

    id: uuid.UUID
    event_type: str
    ride_data: dict
    created_at: datetime


class RideEventsListResponse(BaseModel):
    """Paginated list response for GET /rides/events (offset-based)."""

    events: list[RideEventResponse]
    total: int
    limit: int
    offset: int
