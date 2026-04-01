"""Email template model for localized email texts."""

from datetime import datetime

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class EmailTemplate(Base):
    """Localized email template per email type."""

    __tablename__ = "email_templates"

    email_type: Mapped[str] = mapped_column(String(50), primary_key=True)
    subject_en: Mapped[str] = mapped_column(String(200), nullable=False)
    body_en: Mapped[str] = mapped_column(Text, nullable=False)
    subject_es: Mapped[str] = mapped_column(String(200), nullable=False)
    body_es: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __str__(self) -> str:
        return self.email_type
