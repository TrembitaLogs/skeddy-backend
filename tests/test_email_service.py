import logging
from email.message import EmailMessage
from unittest.mock import AsyncMock, patch

import aiosmtplib
import pytest

from app.services.email_service import send_password_reset_code


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
