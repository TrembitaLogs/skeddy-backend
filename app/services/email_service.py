import logging
from email.message import EmailMessage

import aiosmtplib

from app.config import settings

logger = logging.getLogger(__name__)

RESET_URL_BASE = "https://skeddy.net/reset-password"


async def send_reset_email(to_email: str, reset_token: str) -> None:
    """Send a password reset email with a link containing the reset token.

    Args:
        to_email: Recipient email address.
        reset_token: Plain-text UUID token to include in the reset URL.

    Raises:
        aiosmtplib.SMTPException: On any SMTP failure (logged before re-raise).
    """
    reset_url = f"{RESET_URL_BASE}?token={reset_token}"

    message = EmailMessage()
    message["From"] = settings.EMAIL_FROM
    message["To"] = to_email
    message["Subject"] = "Reset Your Skeddy Password"
    message.set_content(
        f"Click the link below to reset your password:\n"
        f"{reset_url}\n\n"
        f"This link expires in 1 hour.\n"
        f"If you did not request a password reset, please ignore this email."
    )

    try:
        await aiosmtplib.send(
            message,
            hostname=settings.EMAIL_HOST,
            port=settings.EMAIL_PORT,
            username=settings.EMAIL_USER,
            password=settings.EMAIL_PASSWORD,
            start_tls=True,
        )
        logger.info("Reset email sent to %s", to_email)
    except aiosmtplib.SMTPException:
        logger.error("Failed to send reset email to %s", to_email, exc_info=True)
        raise
