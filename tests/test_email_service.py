import logging
from email.message import EmailMessage
from unittest.mock import AsyncMock, patch

import aiosmtplib
import pytest

from app.services.email_service import RESET_URL_BASE, send_reset_email


class TestSendResetEmailMessage:
    """Verify that send_reset_email builds a correct EmailMessage."""

    async def test_email_message_fields(self):
        """EmailMessage has correct From, To, Subject and body content."""
        with patch(
            "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
        ) as mock_send:
            await send_reset_email("driver@example.com", "abc-token-123")

            mock_send.assert_called_once()
            msg: EmailMessage = mock_send.call_args.args[0]

            assert msg["From"] is not None
            assert msg["To"] == "driver@example.com"
            assert msg["Subject"] == "Reset Your Skeddy Password"

            body = msg.get_content()
            assert "abc-token-123" in body
            assert "1 hour" in body

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

            await send_reset_email("user@example.com", "token-456")

            _, kwargs = mock_send.call_args
            assert kwargs["hostname"] == "smtp.test.com"
            assert kwargs["port"] == 465
            assert kwargs["username"] == "testuser"
            assert kwargs["password"] == "testpass"
            assert kwargs["start_tls"] is True


class TestResetUrlFormation:
    """Verify reset URL is formed correctly with different tokens."""

    async def test_simple_token(self):
        """Standard UUID-like token produces correct URL."""
        with patch(
            "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
        ) as mock_send:
            token = "550e8400-e29b-41d4-a716-446655440000"
            await send_reset_email("user@test.com", token)

            msg: EmailMessage = mock_send.call_args.args[0]
            body = msg.get_content()
            expected_url = f"{RESET_URL_BASE}?token={token}"
            assert expected_url in body

    async def test_different_token_values(self):
        """Each call embeds the specific token it received."""
        with patch(
            "app.services.email_service.aiosmtplib.send", new_callable=AsyncMock
        ) as mock_send:
            await send_reset_email("a@b.com", "token-aaa")
            body_1 = mock_send.call_args.args[0].get_content()

            await send_reset_email("c@d.com", "token-bbb")
            body_2 = mock_send.call_args.args[0].get_content()

            assert "token-aaa" in body_1
            assert "token-bbb" not in body_1
            assert "token-bbb" in body_2
            assert "token-aaa" not in body_2


class TestSendResetEmailSmtpErrors:
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
            await send_reset_email("user@test.com", "token-123")

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
            await send_reset_email("user@test.com", "token-456")

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
            await send_reset_email("user@test.com", "token-789")

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
                await send_reset_email("fail@test.com", "token-err")

            error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
            assert len(error_records) == 1
            assert "fail@test.com" in error_records[0].message

    async def test_success_is_logged(self, caplog):
        """Successful send is logged at INFO level."""
        with patch("app.services.email_service.aiosmtplib.send", new_callable=AsyncMock):
            with caplog.at_level(logging.INFO, logger="app.services.email_service"):
                await send_reset_email("ok@test.com", "token-ok")

            info_records = [r for r in caplog.records if r.levelno == logging.INFO]
            assert len(info_records) == 1
            assert "ok@test.com" in info_records[0].message
