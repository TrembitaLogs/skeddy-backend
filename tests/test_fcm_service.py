import logging
import uuid
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from firebase_admin import exceptions, messaging
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.fcm_service import (
    clear_fcm_token,
    initialize_firebase,
    send_credits_depleted,
    send_push,
    send_ride_credit_refunded,
)


@pytest.fixture(autouse=True)
def _reset_firebase_apps():
    """Ensure firebase_admin._apps is clean before and after each test."""
    with patch("app.services.fcm_service.firebase_admin") as mock_fa:
        mock_fa._apps = {}
        mock_fa.initialize_app = MagicMock()
        yield mock_fa
        mock_fa._apps = {}


class TestInitializeFirebaseWithFilePath:
    """Test initialization via FIREBASE_CREDENTIALS_PATH."""

    def test_initializes_with_file_path(self, _reset_firebase_apps):
        mock_fa = _reset_firebase_apps
        mock_cred = MagicMock()

        with (
            patch(
                "app.services.fcm_service.credentials.Certificate", return_value=mock_cred
            ) as mock_cert,
            patch("app.services.fcm_service.settings") as mock_settings,
        ):
            mock_settings.FIREBASE_CREDENTIALS_PATH = "/path/to/creds.json"
            mock_settings.FIREBASE_CREDENTIALS_JSON = ""

            initialize_firebase()

            mock_cert.assert_called_once_with("/path/to/creds.json")
            mock_fa.initialize_app.assert_called_once_with(mock_cred)


class TestInitializeFirebaseWithJSON:
    """Test initialization via FIREBASE_CREDENTIALS_JSON."""

    def test_initializes_with_json_string(self, _reset_firebase_apps):
        mock_fa = _reset_firebase_apps
        mock_cred = MagicMock()
        json_str = '{"type": "service_account", "project_id": "test"}'

        with (
            patch(
                "app.services.fcm_service.credentials.Certificate", return_value=mock_cred
            ) as mock_cert,
            patch("app.services.fcm_service.settings") as mock_settings,
        ):
            mock_settings.FIREBASE_CREDENTIALS_PATH = ""
            mock_settings.FIREBASE_CREDENTIALS_JSON = json_str

            initialize_firebase()

            mock_cert.assert_called_once_with({"type": "service_account", "project_id": "test"})
            mock_fa.initialize_app.assert_called_once_with(mock_cred)

    def test_file_path_takes_priority_over_json(self, _reset_firebase_apps):
        _ = _reset_firebase_apps
        mock_cred = MagicMock()

        with (
            patch(
                "app.services.fcm_service.credentials.Certificate", return_value=mock_cred
            ) as mock_cert,
            patch("app.services.fcm_service.settings") as mock_settings,
        ):
            mock_settings.FIREBASE_CREDENTIALS_PATH = "/path/to/creds.json"
            mock_settings.FIREBASE_CREDENTIALS_JSON = '{"type": "service_account"}'

            initialize_firebase()

            # File path should be used, not JSON
            mock_cert.assert_called_once_with("/path/to/creds.json")


class TestInitializeFirebaseReinitialization:
    """Test that re-initialization is skipped when already initialized."""

    def test_skips_when_already_initialized(self, _reset_firebase_apps):
        mock_fa = _reset_firebase_apps
        mock_fa._apps = {"[DEFAULT]": MagicMock()}

        with (
            patch("app.services.fcm_service.credentials.Certificate") as mock_cert,
            patch("app.services.fcm_service.settings") as mock_settings,
        ):
            mock_settings.FIREBASE_CREDENTIALS_PATH = "/path/to/creds.json"
            mock_settings.FIREBASE_CREDENTIALS_JSON = ""

            initialize_firebase()

            mock_cert.assert_not_called()
            mock_fa.initialize_app.assert_not_called()


class TestInitializeFirebaseNoCredentials:
    """Test ValueError when no credentials are configured."""

    def test_raises_value_error_when_no_credentials(self, _reset_firebase_apps):
        with patch("app.services.fcm_service.settings") as mock_settings:
            mock_settings.FIREBASE_CREDENTIALS_PATH = ""
            mock_settings.FIREBASE_CREDENTIALS_JSON = ""

            with pytest.raises(ValueError, match="Firebase credentials not configured"):
                initialize_firebase()

    def test_raises_value_error_when_none_credentials(self, _reset_firebase_apps):
        with patch("app.services.fcm_service.settings") as mock_settings:
            mock_settings.FIREBASE_CREDENTIALS_PATH = None
            mock_settings.FIREBASE_CREDENTIALS_JSON = None

            with pytest.raises(ValueError, match="Firebase credentials not configured"):
                initialize_firebase()


class TestSendPushSuccess:
    """Test successful push notification delivery."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    async def test_successful_send_first_attempt(self, user_id):
        """Send succeeds on first attempt, returns True."""
        with patch("app.services.fcm_service.messaging.send") as mock_send:
            mock_send.return_value = "projects/test/messages/123"

            result = await send_push(
                db=AsyncMock(spec=AsyncSession),
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123", "price": 25.50},
                user_id=user_id,
            )

            assert result is True
            mock_send.assert_called_once()


class TestSendPushRetry:
    """Test retry logic with exponential backoff."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    async def test_retry_succeeds_on_second_attempt(self, user_id):
        """First attempt fails with network error, second succeeds."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_send.side_effect = [
                ConnectionError("Network error"),
                "projects/test/messages/123",
            ]

            result = await send_push(
                db=AsyncMock(spec=AsyncSession),
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123"},
                user_id=user_id,
            )

            assert result is True
            assert mock_send.call_count == 2
            mock_sleep.assert_called_once_with(1)  # 3^0 = 1s

    async def test_all_three_attempts_fail_returns_false(self, user_id):
        """All 3 attempts fail, returns False."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_send.side_effect = ConnectionError("Network error")

            result = await send_push(
                db=AsyncMock(spec=AsyncSession),
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123"},
                user_id=user_id,
            )

            assert result is False
            assert mock_send.call_count == 3
            assert mock_sleep.call_count == 2
            mock_sleep.assert_has_calls([call(1), call(3)])

    async def test_exponential_backoff_timing(self, user_id):
        """Verify exact backoff sequence: 3^0=1s, 3^1=3s."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_send.side_effect = ConnectionError("Network error")

            await send_push(
                db=AsyncMock(spec=AsyncSession),
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123"},
                user_id=user_id,
            )

            assert mock_sleep.call_args_list == [call(1), call(3)]


class TestSendPushDataConversion:
    """Test that data payload values are converted to strings."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    async def test_data_values_converted_to_strings(self, user_id):
        """All data values must be strings (FCM requirement)."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch("app.services.fcm_service.messaging.Message") as mock_message_cls,
        ):
            mock_send.return_value = "projects/test/messages/123"

            await send_push(
                db=AsyncMock(spec=AsyncSession),
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={
                    "ride_id": "abc-123",
                    "price": 25.50,
                    "count": 3,
                    "flag": True,
                },
                user_id=user_id,
            )

            msg_call = mock_message_cls.call_args
            data = msg_call.kwargs["data"]
            assert data["ride_id"] == "abc-123"
            assert data["price"] == "25.5"
            assert data["count"] == "3"
            assert data["flag"] == "True"
            assert data["type"] == "RIDE_ACCEPTED"
            assert msg_call.kwargs["token"] == "test_token"


# --- Task 5.3: FCM error handling tests ---


class TestSendPushUnregisteredError:
    """Test UnregisteredError handling — clears token, no retry."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    async def test_clears_token_and_returns_false(self, user_id):
        """UnregisteredError triggers clear_fcm_token and returns False."""
        mock_db = AsyncMock(spec=AsyncSession)

        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch(
                "app.services.fcm_service.clear_fcm_token",
                new_callable=AsyncMock,
            ) as mock_clear,
        ):
            mock_send.side_effect = messaging.UnregisteredError("Token not registered")

            result = await send_push(
                db=mock_db,
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123"},
                user_id=user_id,
            )

            assert result is False
            mock_clear.assert_called_once_with(mock_db, user_id)

    async def test_no_retry(self, user_id):
        """UnregisteredError should NOT trigger retries."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch(
                "app.services.fcm_service.clear_fcm_token",
                new_callable=AsyncMock,
            ),
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_send.side_effect = messaging.UnregisteredError("Token not registered")

            await send_push(
                db=AsyncMock(spec=AsyncSession),
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123"},
                user_id=user_id,
            )

            mock_send.assert_called_once()
            mock_sleep.assert_not_called()


class TestSendPushInvalidArgumentError:
    """Test InvalidArgumentError handling — clears token, no retry."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    async def test_clears_token_and_returns_false(self, user_id):
        """InvalidArgumentError triggers clear_fcm_token and returns False."""
        mock_db = AsyncMock(spec=AsyncSession)

        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch(
                "app.services.fcm_service.clear_fcm_token",
                new_callable=AsyncMock,
            ) as mock_clear,
        ):
            mock_send.side_effect = exceptions.InvalidArgumentError("Invalid registration")

            result = await send_push(
                db=mock_db,
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123"},
                user_id=user_id,
            )

            assert result is False
            mock_clear.assert_called_once_with(mock_db, user_id)

    async def test_no_retry(self, user_id):
        """InvalidArgumentError should NOT trigger retries."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch(
                "app.services.fcm_service.clear_fcm_token",
                new_callable=AsyncMock,
            ),
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_send.side_effect = exceptions.InvalidArgumentError("Invalid registration")

            await send_push(
                db=AsyncMock(spec=AsyncSession),
                fcm_token="test_token",
                notification_type="RIDE_ACCEPTED",
                data={"ride_id": "abc-123"},
                user_id=user_id,
            )

            mock_send.assert_called_once()
            mock_sleep.assert_not_called()


class TestClearFcmToken:
    """Test clear_fcm_token updates DB."""

    async def test_clears_fcm_token_in_database(self):
        """Verify execute and commit are called on the session."""
        mock_db = AsyncMock(spec=AsyncSession)
        user_id = uuid.uuid4()

        await clear_fcm_token(mock_db, user_id)

        mock_db.execute.assert_called_once()
        mock_db.commit.assert_called_once()


class TestSendPushErrorLogging:
    """Verify logging calls for each error type."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    async def test_unregistered_error_logs_warning(self, user_id, caplog):
        """UnregisteredError should be logged at WARNING level."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch(
                "app.services.fcm_service.clear_fcm_token",
                new_callable=AsyncMock,
            ),
        ):
            mock_send.side_effect = messaging.UnregisteredError("Token not registered")

            with caplog.at_level(logging.WARNING, logger="app.services.fcm_service"):
                await send_push(
                    db=AsyncMock(spec=AsyncSession),
                    fcm_token="test_token",
                    notification_type="RIDE_ACCEPTED",
                    data={"ride_id": "abc-123"},
                    user_id=user_id,
                )

            warning_records = [
                r
                for r in caplog.records
                if r.levelno == logging.WARNING and "unregistered" in r.message.lower()
            ]
            assert len(warning_records) == 1

    async def test_invalid_argument_logs_warning(self, user_id, caplog):
        """InvalidArgumentError should be logged at WARNING level."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch(
                "app.services.fcm_service.clear_fcm_token",
                new_callable=AsyncMock,
            ),
        ):
            mock_send.side_effect = exceptions.InvalidArgumentError("Invalid registration")

            with caplog.at_level(logging.WARNING, logger="app.services.fcm_service"):
                await send_push(
                    db=AsyncMock(spec=AsyncSession),
                    fcm_token="test_token",
                    notification_type="RIDE_ACCEPTED",
                    data={"ride_id": "abc-123"},
                    user_id=user_id,
                )

            warning_records = [
                r
                for r in caplog.records
                if r.levelno == logging.WARNING and "invalid" in r.message.lower()
            ]
            assert len(warning_records) == 1

    async def test_network_error_logs_error_after_retries(self, user_id, caplog):
        """Network errors should log ERROR after all retries exhausted."""
        with (
            patch("app.services.fcm_service.messaging.send") as mock_send,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            mock_send.side_effect = ConnectionError("Network error")

            with caplog.at_level(logging.WARNING, logger="app.services.fcm_service"):
                await send_push(
                    db=AsyncMock(spec=AsyncSession),
                    fcm_token="test_token",
                    notification_type="RIDE_ACCEPTED",
                    data={"ride_id": "abc-123"},
                    user_id=user_id,
                )

            error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
            assert len(error_records) == 1
            assert "3 attempts" in error_records[0].message


# --- Task 12.1: send_credits_depleted tests ---


class TestSendCreditsDepleted:
    """Test send_credits_depleted helper function."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    async def test_sends_push_with_correct_payload(self, user_id):
        """Calls send_push with CREDITS_DEPLETED type and balance='0'."""
        mock_db = AsyncMock(spec=AsyncSession)
        # Simulate _get_user_push_info query returning (fcm_token, language)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.fcm_token = "test-fcm-token"
        mock_row.language = "en"
        mock_result.one_or_none.return_value = mock_row
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_send:
            await send_credits_depleted(mock_db, user_id)

            mock_send.assert_called_once_with(
                mock_db,
                "test-fcm-token",
                "CREDITS_DEPLETED",
                {"balance": "0"},
                user_id,
                "en",
            )

    async def test_all_payload_values_are_strings(self, user_id):
        """FCM data payload must have all string values."""
        mock_db = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.fcm_token = "test-fcm-token"
        mock_row.language = "en"
        mock_result.one_or_none.return_value = mock_row
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_send:
            await send_credits_depleted(mock_db, user_id)

            payload = mock_send.call_args[0][3]
            for value in payload.values():
                assert isinstance(value, str)

    async def test_skips_when_no_fcm_token(self, user_id):
        """Does not call send_push if user has no FCM token."""
        mock_db = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.fcm_token = None
        mock_row.language = "en"
        mock_result.one_or_none.return_value = mock_row
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
        ) as mock_send:
            await send_credits_depleted(mock_db, user_id)

            mock_send.assert_not_called()

    async def test_does_not_raise_on_fcm_failure(self, user_id):
        """Fire-and-forget: FCM exceptions do not propagate."""
        mock_db = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.fcm_token = "test-fcm-token"
        mock_row.language = "en"
        mock_result.one_or_none.return_value = mock_row
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
            side_effect=OSError("FCM down"),
        ):
            # Should not raise
            await send_credits_depleted(mock_db, user_id)


# --- Task 12.3: send_ride_credit_refunded tests ---


class TestSendRideCreditRefunded:
    """Test send_ride_credit_refunded helper function."""

    @pytest.fixture
    def user_id(self):
        return uuid.uuid4()

    @pytest.fixture
    def ride_id(self):
        return uuid.uuid4()

    async def test_sends_push_with_correct_payload(self, user_id, ride_id):
        """Calls send_push with RIDE_CREDIT_REFUNDED type and correct data."""
        mock_db = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.fcm_token = "test-fcm-token"
        mock_row.language = "en"
        mock_result.one_or_none.return_value = mock_row
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_send:
            await send_ride_credit_refunded(mock_db, user_id, ride_id, 2, 15)

            mock_send.assert_called_once_with(
                mock_db,
                "test-fcm-token",
                "RIDE_CREDIT_REFUNDED",
                {
                    "ride_id": str(ride_id),
                    "credits_refunded": "2",
                    "new_balance": "15",
                },
                user_id,
                "en",
            )

    async def test_all_payload_values_are_strings(self, user_id, ride_id):
        """FCM data payload must have all string values."""
        mock_db = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.fcm_token = "test-fcm-token"
        mock_row.language = "en"
        mock_result.one_or_none.return_value = mock_row
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_send:
            await send_ride_credit_refunded(mock_db, user_id, ride_id, 3, 10)

            payload = mock_send.call_args[0][3]
            for value in payload.values():
                assert isinstance(value, str)

    async def test_skips_when_no_fcm_token(self, user_id, ride_id):
        """Does not call send_push if user has no FCM token."""
        mock_db = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.fcm_token = None
        mock_row.language = "en"
        mock_result.one_or_none.return_value = mock_row
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
        ) as mock_send:
            await send_ride_credit_refunded(mock_db, user_id, ride_id, 2, 15)

            mock_send.assert_not_called()

    async def test_does_not_raise_on_fcm_failure(self, user_id, ride_id):
        """Fire-and-forget: FCM exceptions do not propagate."""
        mock_db = AsyncMock(spec=AsyncSession)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = "test-fcm-token"
        mock_db.execute.return_value = mock_result

        with patch(
            "app.services.fcm_service.send_push",
            new_callable=AsyncMock,
            side_effect=OSError("FCM down"),
        ):
            # Should not raise
            await send_ride_credit_refunded(mock_db, user_id, ride_id, 2, 15)
