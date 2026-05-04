import logging
from email.message import EmailMessage
from unittest.mock import AsyncMock, patch

import aiosmtplib
import pytest

from app.services.email_service import send_password_reset_code, send_welcome_email


class TestSendPasswordResetCodeMessage:
    """Verify that send_password_reset_code builds a correct EmailMessage."""

    async def test_email_message_fields(self):
        """EmailMessage has correct From, To, Subject and body content."""
        with patch(
            "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
        ) as mock_send:
            await send_password_reset_code("driver@example.com", "847291")

            mock_send.assert_called_once()
            msg: EmailMessage = mock_send.call_args.args[0]

            assert msg["From"] is not None
            assert msg["To"] == "driver@example.com"
            assert msg["Subject"] == "Your Skeddy password reset code"

            body = msg.get_content()
            assert "847291" in body
            assert "15 minutes" in body

    async def test_smtp_settings_passed(self):
        """SMTP connection settings from config are forwarded to aiosmtplib.send."""
        with (
            patch(
                "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
            ) as mock_send,
            patch("app.services.email_service.settings") as mock_settings,
        ):
            mock_settings.EMAIL_FROM = "Skeddy <noreply@skeddy.app>"
            mock_settings.EMAIL_HOST = "smtp.test.com"
            mock_settings.EMAIL_PORT = 465
            mock_settings.EMAIL_USER = "testuser"
            mock_settings.EMAIL_PASSWORD = "testpass"

            await send_password_reset_code("user@example.com", "123456")

            _, kwargs = mock_send.call_args
            assert kwargs["hostname"] == "smtp.test.com"
            assert kwargs["port"] == 465
            assert kwargs["username"] == "testuser"
            assert kwargs["password"] == "testpass"
            assert kwargs["start_tls"] is True


class TestResetCodeInBody:
    """Verify reset code is embedded correctly in email body."""

    async def test_code_present_in_body(self):
        """6-digit code appears in email body."""
        with patch(
            "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
        ) as mock_send:
            await send_password_reset_code("user@test.com", "593817")

            msg: EmailMessage = mock_send.call_args.args[0]
            body = msg.get_content()
            assert "593817" in body

    async def test_different_codes(self):
        """Each call embeds the specific code it received."""
        with patch(
            "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
        ) as mock_send:
            await send_password_reset_code("a@b.com", "111111")
            body_1 = mock_send.call_args.args[0].get_content()

            await send_password_reset_code("c@d.com", "222222")
            body_2 = mock_send.call_args.args[0].get_content()

            assert "111111" in body_1
            assert "222222" not in body_1
            assert "222222" in body_2
            assert "111111" not in body_2

    async def test_no_url_in_body(self):
        """Email body must not contain a URL (code-based, not link-based)."""
        with patch(
            "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
        ) as mock_send:
            await send_password_reset_code("user@test.com", "999999")

            msg: EmailMessage = mock_send.call_args.args[0]
            body = msg.get_content()
            assert "https://" not in body
            assert "http://" not in body


class TestSendPasswordResetCodeSmtpErrors:
    """Verify correct error handling for SMTP failures."""

    async def test_connection_refused_raises(self):
        """SMTPConnectError is re-raised after logging."""
        with (
            patch(
                "app.services.email_service.aiosmtplib.send",
                new_callable=AsyncMock,
                side_effect=aiosmtplib.SMTPConnectError("Connection refused"),
            ),
            pytest.raises(aiosmtplib.SMTPConnectError),
        ):
            await send_password_reset_code("user@test.com", "123456")

    async def test_auth_failure_raises(self):
        """SMTPAuthenticationError is re-raised after logging."""
        with (
            patch(
                "app.services.email_service.aiosmtplib.send",
                new_callable=AsyncMock,
                side_effect=aiosmtplib.SMTPAuthenticationError(535, "Auth failed"),
            ),
            pytest.raises(aiosmtplib.SMTPAuthenticationError),
        ):
            await send_password_reset_code("user@test.com", "654321")

    async def test_generic_smtp_error_raises(self):
        """Base SMTPException is re-raised after logging."""
        with (
            patch(
                "app.services.email_service.aiosmtplib.send",
                new_callable=AsyncMock,
                side_effect=aiosmtplib.SMTPException("Something went wrong"),
            ),
            pytest.raises(aiosmtplib.SMTPException),
        ):
            await send_password_reset_code("user@test.com", "789012")

    async def test_error_is_logged(self, caplog):
        """SMTP failures are logged at ERROR level before re-raise."""
        with patch(
            "app.services.email_service.aiosmtplib.send",
            new_callable=AsyncMock,
            side_effect=aiosmtplib.SMTPConnectError("Connection refused"),
        ):
            with (
                caplog.at_level(logging.ERROR, logger="app.services.email_service"),
                pytest.raises(aiosmtplib.SMTPConnectError),
            ):
                await send_password_reset_code("fail@test.com", "000000")

            error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
            assert len(error_records) == 1
            assert "fail@test.com" in error_records[0].message

    async def test_success_is_logged(self, caplog):
        """Successful send is logged at INFO level."""
        with patch("app.services.email_service.aiosmtplib.send", new_callable=AsyncMock):
            with caplog.at_level(logging.INFO, logger="app.services.email_service"):
                await send_password_reset_code("ok@test.com", "555555")

            info_records = [r for r in caplog.records if r.levelno == logging.INFO]
            assert len(info_records) == 1
            assert "ok@test.com" in info_records[0].message


class TestWelcomeFallbackTemplate:
    """Verify _FALLBACK_TEMPLATES includes WELCOME with required placeholders."""

    def test_welcome_fallback_en_present(self):
        from app.services.email_service import _FALLBACK_TEMPLATES

        assert "WELCOME" in _FALLBACK_TEMPLATES
        en = _FALLBACK_TEMPLATES["WELCOME"]["en"]
        assert en["subject"]
        assert "{search_app_url}" in en["body"]
        assert "{bonus_amount}" in en["body"]

    def test_welcome_fallback_es_present(self):
        from app.services.email_service import _FALLBACK_TEMPLATES

        es = _FALLBACK_TEMPLATES["WELCOME"]["es"]
        assert es["subject"]
        assert "{search_app_url}" in es["body"]
        assert "{bonus_amount}" in es["body"]


class TestSendWelcomeEmail:
    """Unit tests for send_welcome_email."""

    async def test_renders_placeholders_en(self):
        """Welcome email body contains the search app URL and bonus amount."""
        with (
            patch(
                "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
            ) as mock_send,
            patch("app.services.email_service.settings") as mock_settings,
        ):
            mock_settings.EMAIL_FROM = "noreply@skeddy.app"
            mock_settings.EMAIL_HOST = "smtp.test"
            mock_settings.EMAIL_PORT = 587
            mock_settings.EMAIL_USER = "u"
            mock_settings.EMAIL_PASSWORD = "p"
            mock_settings.SEARCH_APP_UPDATE_URL = "https://example.com/search.apk"

            await send_welcome_email("driver@example.com", language="en")

            mock_send.assert_called_once()
            msg: EmailMessage = mock_send.call_args.args[0]
            assert msg["To"] == "driver@example.com"
            body = msg.get_content()
            assert "https://example.com/search.apk" in body
            # When db/redis are None, function uses DEFAULT_REGISTRATION_BONUS_CREDITS (10)
            assert "10" in body
            assert msg["Subject"] == "Welcome to Skeddy"

    async def test_uses_spanish_template_when_language_es(self):
        with (
            patch(
                "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
            ) as mock_send,
            patch("app.services.email_service.settings") as mock_settings,
        ):
            mock_settings.EMAIL_FROM = "noreply@skeddy.app"
            mock_settings.EMAIL_HOST = "smtp.test"
            mock_settings.EMAIL_PORT = 587
            mock_settings.EMAIL_USER = "u"
            mock_settings.EMAIL_PASSWORD = "p"
            mock_settings.SEARCH_APP_UPDATE_URL = "https://example.com/search.apk"

            await send_welcome_email("driver@example.com", language="es")

            msg: EmailMessage = mock_send.call_args.args[0]
            assert msg["Subject"] == "Bienvenido a Skeddy"
            body = msg.get_content()
            assert "Bienvenido" in body

    async def test_unknown_language_falls_back_to_english(self):
        with (
            patch(
                "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
            ) as mock_send,
            patch("app.services.email_service.settings") as mock_settings,
        ):
            mock_settings.EMAIL_FROM = "noreply@skeddy.app"
            mock_settings.EMAIL_HOST = "smtp.test"
            mock_settings.EMAIL_PORT = 587
            mock_settings.EMAIL_USER = "u"
            mock_settings.EMAIL_PASSWORD = "p"
            mock_settings.SEARCH_APP_UPDATE_URL = "https://example.com/search.apk"

            await send_welcome_email("driver@example.com", language="fr")

            msg: EmailMessage = mock_send.call_args.args[0]
            assert msg["Subject"] == "Welcome to Skeddy"

    async def test_uses_bonus_from_config_when_db_redis_provided(self):
        """When both db and redis are provided, bonus comes from get_registration_bonus_credits."""
        with (
            patch(
                "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
            ) as mock_send,
            patch("app.services.email_service.settings") as mock_settings,
            patch(
                "app.services.email_service.get_registration_bonus_credits",
                new_callable=AsyncMock,
                return_value=42,
            ),
            patch(
                "app.services.email_service._get_templates",
                new_callable=AsyncMock,
            ) as mock_get_templates,
        ):
            mock_settings.EMAIL_FROM = "noreply@skeddy.app"
            mock_settings.EMAIL_HOST = "smtp.test"
            mock_settings.EMAIL_PORT = 587
            mock_settings.EMAIL_USER = "u"
            mock_settings.EMAIL_PASSWORD = "p"
            mock_settings.SEARCH_APP_UPDATE_URL = "https://example.com/search.apk"
            mock_get_templates.return_value = {
                "WELCOME": {
                    "en": {
                        "subject": "Welcome",
                        "body": "Bonus: {bonus_amount}, URL: {search_app_url}",
                    },
                },
            }

            db_mock = object()
            redis_mock = object()
            await send_welcome_email(
                "driver@example.com", language="en", db=db_mock, redis=redis_mock
            )

            msg: EmailMessage = mock_send.call_args.args[0]
            body = msg.get_content()
            assert "Bonus: 42" in body
            assert "URL: https://example.com/search.apk" in body

    async def test_falls_back_to_default_bonus_when_db_or_redis_none(self):
        """If db or redis is None, function uses DEFAULT_REGISTRATION_BONUS_CREDITS (10)."""
        with (
            patch(
                "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
            ) as mock_send,
            patch("app.services.email_service.settings") as mock_settings,
        ):
            mock_settings.EMAIL_FROM = "noreply@skeddy.app"
            mock_settings.EMAIL_HOST = "smtp.test"
            mock_settings.EMAIL_PORT = 587
            mock_settings.EMAIL_USER = "u"
            mock_settings.EMAIL_PASSWORD = "p"
            mock_settings.SEARCH_APP_UPDATE_URL = "https://example.com/search.apk"

            await send_welcome_email("driver@example.com", db=None, redis=None)

            msg: EmailMessage = mock_send.call_args.args[0]
            body = msg.get_content()
            assert "10" in body  # DEFAULT_REGISTRATION_BONUS_CREDITS

    async def test_falls_back_to_in_code_templates_when_get_templates_returns_fallback(
        self,
    ):
        """When _get_templates returns the in-code _FALLBACK_TEMPLATES (e.g. DB/Redis
        failure), send_welcome_email still renders the WELCOME body and sends."""
        from app.services.email_service import _FALLBACK_TEMPLATES

        with (
            patch(
                "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
            ) as mock_send,
            patch("app.services.email_service.settings") as mock_settings,
            patch(
                "app.services.email_service._get_templates",
                new_callable=AsyncMock,
                return_value=_FALLBACK_TEMPLATES,
            ) as mock_get_templates,
            patch(
                "app.services.email_service.get_registration_bonus_credits",
                new_callable=AsyncMock,
                return_value=7,
            ),
        ):
            mock_settings.EMAIL_FROM = "noreply@skeddy.app"
            mock_settings.EMAIL_HOST = "smtp.test"
            mock_settings.EMAIL_PORT = 587
            mock_settings.EMAIL_USER = "u"
            mock_settings.EMAIL_PASSWORD = "p"
            mock_settings.SEARCH_APP_UPDATE_URL = "https://example.com/search.apk"

            db_mock = object()
            redis_mock = object()
            await send_welcome_email(
                "driver@example.com", language="en", db=db_mock, redis=redis_mock
            )

            mock_get_templates.assert_awaited_once_with(db_mock, redis_mock)
            msg: EmailMessage = mock_send.call_args.args[0]
            assert msg["Subject"] == "Welcome to Skeddy"
            body = msg.get_content()
            assert "https://example.com/search.apk" in body
            # Bonus from patched get_registration_bonus_credits = 7
            assert "7 bonus credits" in body
