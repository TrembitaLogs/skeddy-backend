import logging
from email.message import EmailMessage

import aiosmtplib
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fallback templates (used when DB templates are unavailable)
# ---------------------------------------------------------------------------

_FALLBACK_TEMPLATES: dict[str, dict[str, dict[str, str]]] = {
    "VERIFICATION": {
        "en": {
            "subject": "Verify your Skeddy account",
            "body": (
                "Welcome to Skeddy!\n\nYour verification code is:\n\n{code}\n\n"
                "Enter this code in the Skeddy app to verify your email address.\n"
                "This code expires in 24 hours.\n\n"
                "If you didn't create a Skeddy account, you can safely ignore this email.\n\n"
                "\u2014 Skeddy Team"
            ),
        },
    },
    "EMAIL_CHANGE": {
        "en": {
            "subject": "Confirm your new Skeddy email",
            "body": (
                "You requested to change your Skeddy account email to this address.\n\n"
                "Your confirmation code is:\n\n{code}\n\n"
                "Enter this code in the Skeddy app to confirm the change.\n"
                "This code expires in 24 hours.\n\n"
                "If you didn't request this change, you can safely ignore this email.\n\n"
                "\u2014 Skeddy Team"
            ),
        },
    },
    "PASSWORD_RESET": {
        "en": {
            "subject": "Your Skeddy password reset code",
            "body": (
                "Your password reset code is:\n\n{code}\n\n"
                "Enter this code in the Skeddy app to reset your password.\n"
                "This code expires in 15 minutes.\n\n"
                "If you didn't request a password reset, you can safely ignore this email.\n\n"
                "\u2014 Skeddy Team"
            ),
        },
    },
}


def _resolve_template(
    templates: dict[str, dict[str, dict[str, str]]],
    email_type: str,
    language: str,
) -> dict[str, str]:
    """Resolve a template by type and language with fallback to English, then to hardcoded."""
    type_templates = templates.get(email_type, {})
    template = type_templates.get(language) or type_templates.get("en")
    if template:
        return template
    # Fall back to hardcoded defaults
    fallback = _FALLBACK_TEMPLATES.get(email_type, {})
    return fallback.get(language) or fallback.get("en", {"subject": "", "body": ""})


async def _send_email(to_email: str, subject: str, body: str, log_label: str) -> None:
    """Send an email via SMTP."""
    message = EmailMessage()
    message["From"] = settings.EMAIL_FROM
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body)

    try:
        await aiosmtplib.send(
            message,
            hostname=settings.EMAIL_HOST,
            port=settings.EMAIL_PORT,
            username=settings.EMAIL_USER,
            password=settings.EMAIL_PASSWORD,
            start_tls=True,
        )
        logger.info("%s sent to %s", log_label, to_email)
    except aiosmtplib.SMTPException:
        logger.error("Failed to send %s to %s", log_label, to_email, exc_info=True)
        raise


async def _get_templates(db: AsyncSession | None, redis: Redis | None) -> dict:
    """Load templates from DB/cache if available, otherwise use fallbacks."""
    if db is not None and redis is not None:
        try:
            from app.services.config_service import get_email_templates

            return await get_email_templates(db, redis)
        except (OSError, RedisError) as exc:
            logger.warning("Failed to load email templates from DB, using fallbacks: %s", exc)
    return _FALLBACK_TEMPLATES


async def _send_code_email(
    to_email: str,
    code: str,
    template_type: str,
    log_label: str,
    language: str = "en",
    db: AsyncSession | None = None,
    redis: Redis | None = None,
) -> None:
    """Send a templated code email (shared logic for all code-based emails)."""
    templates = await _get_templates(db, redis)
    t = _resolve_template(templates, template_type, language)
    await _send_email(to_email, t["subject"], t["body"].format(code=code), log_label)


async def send_password_reset_code(
    to_email: str,
    code: str,
    language: str = "en",
    db: AsyncSession | None = None,
    redis: Redis | None = None,
) -> None:
    """Send a password reset email containing a verification code."""
    await _send_code_email(
        to_email, code, "PASSWORD_RESET", "Password reset code", language, db, redis
    )


async def send_email_change_code(
    to_email: str,
    code: str,
    language: str = "en",
    db: AsyncSession | None = None,
    redis: Redis | None = None,
) -> None:
    """Send a verification code for email change request."""
    await _send_code_email(
        to_email, code, "EMAIL_CHANGE", "Email change code", language, db, redis
    )


async def send_verification_code(
    to_email: str,
    code: str,
    language: str = "en",
    db: AsyncSession | None = None,
    redis: Redis | None = None,
) -> None:
    """Send an email verification code after registration."""
    await _send_code_email(
        to_email, code, "VERIFICATION", "Verification code", language, db, redis
    )
