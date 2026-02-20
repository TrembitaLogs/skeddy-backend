import logging
from email.message import EmailMessage

import aiosmtplib

from app.config import settings

logger = logging.getLogger(__name__)


async def send_password_reset_code(to_email: str, code: str) -> None:
    """Send a password reset email containing a 6-digit verification code.

    Args:
        to_email: Recipient email address.
        code: Plain-text 6-digit code to include in the email body.

    Raises:
        aiosmtplib.SMTPException: On any SMTP failure (logged before re-raise).
    """
    message = EmailMessage()
    message["From"] = settings.EMAIL_FROM
    message["To"] = to_email
    message["Subject"] = "Your Skeddy password reset code"
    message.set_content(
        f"Your password reset code is:\n\n"
        f"{code}\n\n"
        f"Enter this code in the Skeddy app to reset your password.\n"
        f"This code expires in 15 minutes.\n\n"
        f"If you didn't request a password reset, you can safely ignore this email.\n\n"
        f"\u2014 Skeddy Team"
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
        logger.info("Password reset code sent to %s", to_email)
    except aiosmtplib.SMTPException:
        logger.error("Failed to send reset email to %s", to_email, exc_info=True)
        raise


async def send_verification_code(to_email: str, code: str) -> None:
    """Send an email verification code after registration.

    Args:
        to_email: Recipient email address.
        code: Plain-text 6-digit code to include in the email body.

    Raises:
        aiosmtplib.SMTPException: On any SMTP failure (logged before re-raise).
    """
    message = EmailMessage()
    message["From"] = settings.EMAIL_FROM
    message["To"] = to_email
    message["Subject"] = "Verify your Skeddy account"
    message.set_content(
        f"Welcome to Skeddy!\n\n"
        f"Your verification code is:\n\n"
        f"{code}\n\n"
        f"Enter this code in the Skeddy app to verify your email address.\n"
        f"This code expires in 24 hours.\n\n"
        f"If you didn't create a Skeddy account, you can safely ignore this email.\n\n"
        f"\u2014 Skeddy Team"
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
        logger.info("Verification code sent to %s", to_email)
    except aiosmtplib.SMTPException:
        logger.error("Failed to send verification email to %s", to_email, exc_info=True)
        raise
