import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class VerificationStatus(StrEnum):
    """Application-level enum for ride verification statuses.

    Stored as VARCHAR(20) in DB for easy extensibility.
    """

    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


class Ride(Base):
    """Ride event model with JSONB data storage."""

    __tablename__ = "rides"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(String(36), nullable=False)
    event_type: Mapped[str] = mapped_column(String(20), nullable=False)
    ride_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Billing and verification fields
    ride_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    verification_status: Mapped[str | None] = mapped_column(String(20), server_default="PENDING")
    verification_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    disappeared_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_reported_present: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_verification_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    credits_charged: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    credits_refunded: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )

    user = relationship("User", back_populates="rides")

    __table_args__ = (
        Index("idx_rides_user_created", "user_id", created_at.desc()),
        UniqueConstraint("user_id", "idempotency_key", name="idx_rides_idempotency"),
        Index(
            "idx_rides_verification",
            "verification_status",
            "verification_deadline",
            postgresql_where=text("verification_status = 'PENDING'"),
        ),
        Index("idx_rides_ride_hash", "ride_hash"),
        CheckConstraint("credits_charged >= 0", name="ck_rides_credits_charged_non_negative"),
        CheckConstraint("credits_refunded >= 0", name="ck_rides_credits_refunded_non_negative"),
    )
