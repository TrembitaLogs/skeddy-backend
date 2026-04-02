import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class PairedDevice(Base):
    """Paired search device model. One device per user."""

    __tablename__ = "paired_devices"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    device_id: Mapped[str] = mapped_column(String(255), nullable=False)
    device_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    app_version: Mapped[str | None] = mapped_column(String(20), nullable=True)
    device_token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_ping_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_interval_sent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    timezone: Mapped[str] = mapped_column(String(50), nullable=False)
    offline_notified: Mapped[bool] = mapped_column(Boolean, default=False)
    accessibility_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    lyft_running: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    screen_on: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    location_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    user = relationship("User", back_populates="paired_device")

    __table_args__ = (
        Index("idx_paired_devices_device_id", "device_id", unique=True),
        Index("idx_paired_devices_token_hash", "device_token_hash"),
    )
