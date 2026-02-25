from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class FcmRegisterRequest(BaseModel):
    """FCM token registration request schema."""

    fcm_token: str = Field(min_length=1, max_length=500)


class NotificationType(StrEnum):
    """FCM notification type identifiers sent in data payload."""

    RIDE_ACCEPTED = "RIDE_ACCEPTED"
    SEARCH_OFFLINE = "SEARCH_OFFLINE"
    CREDITS_DEPLETED = "CREDITS_DEPLETED"
    CREDITS_LOW = "CREDITS_LOW"
    RIDE_CREDIT_REFUNDED = "RIDE_CREDIT_REFUNDED"


class RideAcceptedData(BaseModel):
    """Validation model for RIDE_ACCEPTED notification data payload."""

    ride_id: UUID
    price: float
    pickup_time: str
    pickup_location: str
    dropoff_location: str


class SearchOfflineData(BaseModel):
    """Validation model for SEARCH_OFFLINE notification data payload."""

    device_id: str
    last_ping_at: datetime


def create_ride_accepted_payload(
    ride_id: UUID,
    price: float,
    pickup_time: str,
    pickup_location: str,
    dropoff_location: str,
) -> dict[str, str]:
    """Create a validated, FCM-compatible data payload for RIDE_ACCEPTED.

    Validates inputs via Pydantic and converts all values to strings.
    """
    data = RideAcceptedData(
        ride_id=ride_id,
        price=price,
        pickup_time=pickup_time,
        pickup_location=pickup_location,
        dropoff_location=dropoff_location,
    )
    return {
        "ride_id": str(data.ride_id),
        "price": str(data.price),
        "pickup_time": data.pickup_time,
        "pickup_location": data.pickup_location,
        "dropoff_location": data.dropoff_location,
    }


def create_credits_depleted_payload() -> dict[str, str]:
    """Create an FCM-compatible data payload for CREDITS_DEPLETED.

    All values are strings (FCM data payload requirement).
    """
    return {"balance": "0"}


class CreditsLowData(BaseModel):
    """Validation model for CREDITS_LOW notification data payload."""

    balance: int
    threshold: int


def create_credits_low_payload(
    balance: int,
    threshold: int,
) -> dict[str, str]:
    """Create a validated, FCM-compatible data payload for CREDITS_LOW.

    Validates inputs via Pydantic and converts all values to strings.
    """
    data = CreditsLowData(balance=balance, threshold=threshold)
    return {
        "balance": str(data.balance),
        "threshold": str(data.threshold),
    }


class RideCreditRefundedData(BaseModel):
    """Validation model for RIDE_CREDIT_REFUNDED notification data payload."""

    ride_id: UUID
    credits_refunded: int
    new_balance: int


def create_ride_credit_refunded_payload(
    ride_id: UUID,
    credits_refunded: int,
    new_balance: int,
) -> dict[str, str]:
    """Create a validated, FCM-compatible data payload for RIDE_CREDIT_REFUNDED.

    Validates inputs via Pydantic and converts all values to strings.
    """
    data = RideCreditRefundedData(
        ride_id=ride_id,
        credits_refunded=credits_refunded,
        new_balance=new_balance,
    )
    return {
        "ride_id": str(data.ride_id),
        "credits_refunded": str(data.credits_refunded),
        "new_balance": str(data.new_balance),
    }


def create_search_offline_payload(
    device_id: str,
    last_ping_at: datetime,
) -> dict[str, str]:
    """Create a validated, FCM-compatible data payload for SEARCH_OFFLINE.

    Validates inputs via Pydantic and converts all values to strings.
    """
    data = SearchOfflineData(
        device_id=device_id,
        last_ping_at=last_ping_at,
    )
    return {
        "device_id": data.device_id,
        "last_ping_at": data.last_ping_at.isoformat(),
    }
