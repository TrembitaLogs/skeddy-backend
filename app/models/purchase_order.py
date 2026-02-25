import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class PurchaseStatus(StrEnum):
    """Application-level enum for purchase order statuses.

    Stored as VARCHAR(20) in DB for easy extensibility.
    """

    PENDING = "PENDING"
    CONSUMED = "CONSUMED"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class PurchaseOrder(Base):
    """Google Play purchase verification and credit delivery tracking."""

    __tablename__ = "purchase_orders"
    __table_args__ = (
        CheckConstraint("credits_amount > 0", name="ck_purchase_orders_credits_amount_positive"),
        Index("idx_purchase_orders_user", "user_id"),
        Index(
            "idx_purchase_orders_consumed",
            "status",
            postgresql_where=text("status = 'CONSUMED'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    google_order_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)
    product_id: Mapped[str] = mapped_column(String(100), nullable=False)
    purchase_token: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    credits_amount: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=PurchaseStatus.PENDING.value
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user = relationship("User", back_populates="purchase_orders")
