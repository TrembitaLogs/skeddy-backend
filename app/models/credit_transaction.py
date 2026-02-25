import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class TransactionType(StrEnum):
    """Application-level enum for credit transaction types.

    Stored as VARCHAR(30) in DB for easy extensibility (e.g. PROMO_BONUS).
    """

    REGISTRATION_BONUS = "REGISTRATION_BONUS"
    PURCHASE = "PURCHASE"
    RIDE_CHARGE = "RIDE_CHARGE"
    RIDE_REFUND = "RIDE_REFUND"
    ADMIN_ADJUSTMENT = "ADMIN_ADJUSTMENT"


class CreditTransaction(Base):
    """Immutable audit log of every credit balance change."""

    __tablename__ = "credit_transactions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    type: Mapped[str] = mapped_column(String(30), nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    balance_after: Mapped[int] = mapped_column(Integer, nullable=False)
    reference_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    user = relationship("User", back_populates="credit_transactions")

    __table_args__ = (
        Index("idx_credit_transactions_user_created", "user_id", created_at.desc()),
        Index("idx_credit_transactions_reference", "reference_id"),
    )
