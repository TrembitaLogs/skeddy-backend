import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator


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
    ride_hash: str
    timezone: str
    ride_data: RideData

    @field_validator("idempotency_key")
    @classmethod
    def validate_idempotency_key_uuid(cls, v: str) -> str:
        try:
            uuid.UUID(v)
        except ValueError:
            raise ValueError("idempotency_key must be a valid UUID")
        return v

    @field_validator("ride_hash")
    @classmethod
    def validate_ride_hash(cls, v: str) -> str:
        if len(v) != 64:
            raise ValueError("ride_hash must be exactly 64 characters")
        try:
            int(v, 16)
        except ValueError:
            raise ValueError("ride_hash must be a valid hex string (SHA-256)")
        return v.lower()


class CreateRideResponse(BaseModel):
    """Response schema for POST /rides."""

    ok: bool = True
    ride_id: uuid.UUID


class RideEventResponse(BaseModel):
    """Ride event card in the unified event feed.

    Includes billing fields (credits_charged, credits_refunded,
    verification_status) alongside ride data.
    """

    event_kind: Literal["ride"] = "ride"
    id: uuid.UUID
    event_type: str
    ride_data: dict
    credits_charged: int = 0
    credits_refunded: int = 0
    verification_status: str = "PENDING"
    created_at: datetime


class CreditEventResponse(BaseModel):
    """Credit event card in the unified event feed.

    Represents non-ride credit events: REGISTRATION_BONUS, PURCHASE,
    ADMIN_ADJUSTMENT.  RIDE_CHARGE and RIDE_REFUND are embedded in
    ride cards instead.
    """

    event_kind: Literal["credit"] = "credit"
    id: uuid.UUID
    credit_type: str
    amount: int
    balance_after: int
    description: str | None = None
    created_at: datetime


UnifiedEventResponse = Annotated[
    RideEventResponse | CreditEventResponse,
    Field(discriminator="event_kind"),
]
"""Discriminated union of ride and credit events for the unified feed."""


class EventsListResponse(BaseModel):
    """Cursor-based paginated response for GET /rides/events."""

    events: list[UnifiedEventResponse]
    next_cursor: str | None = None
    has_more: bool


class RideEventsListResponse(BaseModel):
    """Legacy offset-based paginated response for GET /rides/events."""

    events: list[RideEventResponse]
    total: int
    limit: int
    offset: int
