import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class User(Base):
    """User account model."""

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    phone_number: Mapped[str | None] = mapped_column(String(20), unique=True, nullable=True)
    fcm_token: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    refresh_tokens = relationship(
        "RefreshToken", back_populates="user", cascade="all, delete-orphan"
    )
    paired_device = relationship(
        "PairedDevice",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )
    search_filters = relationship(
        "SearchFilters",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )
    search_status = relationship(
        "SearchStatus",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )
    rides = relationship("Ride", back_populates="user", cascade="all, delete-orphan")
    accept_failures = relationship(
        "AcceptFailure", back_populates="user", cascade="all, delete-orphan"
    )
