import re
import uuid

from sqlalchemy import Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from app.database import Base

_DEFAULT_WORKING_DAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
_START_TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


class SearchFilters(Base):
    """Search filter preferences for a user. One row per user."""

    __tablename__ = "search_filters"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    min_price: Mapped[float] = mapped_column(Float, default=20.0)
    start_time: Mapped[str] = mapped_column(String(5), default="06:30")
    working_time: Mapped[int] = mapped_column(Integer, default=24)
    working_days: Mapped[list[str]] = mapped_column(
        ARRAY(String(3)), default=lambda: list(_DEFAULT_WORKING_DAYS)
    )

    user = relationship("User", back_populates="search_filters")

    @validates("start_time")
    def validate_start_time(self, _key: str, value: str) -> str:
        if not _START_TIME_RE.match(value):
            raise ValueError(f"start_time must be in HH:MM 24h format, got: {value!r}")
        return value
