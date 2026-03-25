"""Push notification template model for localized notification texts."""

from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class PushTemplate(Base):
    """Localized push notification template per notification type."""

    __tablename__ = "push_templates"

    notification_type: Mapped[str] = mapped_column(String(50), primary_key=True)
    title_en: Mapped[str] = mapped_column(String(200), nullable=False)
    body_en: Mapped[str] = mapped_column(String(500), nullable=False)
    title_es: Mapped[str] = mapped_column(String(200), nullable=False)
    body_es: Mapped[str] = mapped_column(String(500), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __str__(self) -> str:
        return self.notification_type
