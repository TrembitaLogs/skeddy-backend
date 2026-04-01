import logging
from email.message import EmailMessage

import aiosmtplib
from redis.asyncio import Redis
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
        "es": {
            "subject": "Verifica tu cuenta de Skeddy",
            "body": (
                "\u00a1Bienvenido a Skeddy!\n\nTu c\u00f3digo de verificaci\u00f3n es:\n\n{code}\n\n"
                "Ingresa este c\u00f3digo en la aplicaci\u00f3n Skeddy para verificar tu direcci\u00f3n de correo.\n"
                "Este c\u00f3digo expira en 24 horas.\n\n"
                "Si no creaste una cuenta en Skeddy, puedes ignorar este correo.\n\n"
                "\u2014 Equipo Skeddy"
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
        "es": {
            "subject": "Confirma tu nuevo correo de Skeddy",
            "body": (
                "Solicitaste cambiar el correo de tu cuenta de Skeddy a esta direcci\u00f3n.\n\n"
                "Tu c\u00f3digo de confirmaci\u00f3n es:\n\n{code}\n\n"
                "Ingresa este c\u00f3digo en la aplicaci\u00f3n Skeddy para confirmar el cambio.\n"
                "Este c\u00f3digo expira en 24 horas.\n\n"
                "Si no solicitaste este cambio, puedes ignorar este correo.\n\n"
                "\u2014 Equipo Skeddy"
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
        "es": {
            "subject": "Tu c\u00f3digo de restablecimiento de contrase\u00f1a de Skeddy",
            "body": (
                "Tu c\u00f3digo de restablecimiento de contrase\u00f1a es:\n\n{code}\n\n"
                "Ingresa este c\u00f3digo en la aplicaci\u00f3n Skeddy para restablecer tu contrase\u00f1a.\n"
                "Este c\u00f3digo expira en 15 minutos.\n\n"
                "Si no solicitaste un restablecimiento de contrase\u00f1a, puedes ignorar este correo.\n\n"
                "\u2014 Equipo Skeddy"
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
        except Exception:
            logger.warning("Failed to load email templates from DB, using fallbacks")
    return _FALLBACK_TEMPLATES


async def send_password_reset_code(
    to_email: str,
    code: str,
    language: str = "en",
    db: AsyncSession | None = None,
    redis: Redis | None = None,
) -> None:
    """Send a password reset email containing a 6-digit verification code."""
    templates = await _get_templates(db, redis)
    t = _resolve_template(templates, "PASSWORD_RESET", language)
    await _send_email(to_email, t["subject"], t["body"].format(code=code), "Password reset code")


async def send_email_change_code(
    to_email: str,
    code: str,
    language: str = "en",
    db: AsyncSession | None = None,
    redis: Redis | None = None,
) -> None:
    """Send a verification code for email change request."""
    templates = await _get_templates(db, redis)
    t = _resolve_template(templates, "EMAIL_CHANGE", language)
    await _send_email(to_email, t["subject"], t["body"].format(code=code), "Email change code")


async def send_verification_code(
    to_email: str,
    code: str,
    language: str = "en",
    db: AsyncSession | None = None,
    redis: Redis | None = None,
) -> None:
    """Send an email verification code after registration."""
    templates = await _get_templates(db, redis)
    t = _resolve_template(templates, "VERIFICATION", language)
    await _send_email(to_email, t["subject"], t["body"].format(code=code), "Verification code")
