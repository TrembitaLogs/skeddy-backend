from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from googleapiclient.errors import HttpError

from app.services.google_play_service import (
    SCOPES,
    GooglePlayService,
    GooglePlayVerificationError,
    GooglePurchaseResult,
)

# ---------------------------------------------------------------------------
# Test 1: Initialization with file path credentials
# ---------------------------------------------------------------------------


class TestInitWithFilePath:
    """GooglePlayService loads credentials from GOOGLE_PLAY_CREDENTIALS_PATH."""

    def test_initializes_with_file_path(self):
        mock_creds = MagicMock()

        with (
            patch("app.services.google_play_service.settings") as mock_settings,
            patch(
                "app.services.google_play_service.service_account.Credentials"
                ".from_service_account_file",
                return_value=mock_creds,
            ) as mock_from_file,
            patch(
                "app.services.google_play_service.build",
                return_value=MagicMock(),
            ) as mock_build,
        ):
            mock_settings.GOOGLE_PLAY_CREDENTIALS_PATH = "/path/to/creds.json"
            mock_settings.GOOGLE_PLAY_CREDENTIALS_JSON = ""
            mock_settings.GOOGLE_PLAY_PACKAGE_NAME = "com.skeddy.driver"

            svc = GooglePlayService()

            mock_from_file.assert_called_once_with("/path/to/creds.json", scopes=SCOPES)
            mock_build.assert_called_once_with("androidpublisher", "v3", credentials=mock_creds)
            assert svc._package_name == "com.skeddy.driver"


# ---------------------------------------------------------------------------
# Test 2: Initialization with JSON string credentials
# ---------------------------------------------------------------------------


class TestInitWithJSON:
    """GooglePlayService loads credentials from GOOGLE_PLAY_CREDENTIALS_JSON."""

    def test_initializes_with_json_string(self):
        mock_creds = MagicMock()
        json_str = '{"type": "service_account", "project_id": "test"}'

        with (
            patch("app.services.google_play_service.settings") as mock_settings,
            patch(
                "app.services.google_play_service.service_account.Credentials"
                ".from_service_account_info",
                return_value=mock_creds,
            ) as mock_from_info,
            patch(
                "app.services.google_play_service.build",
                return_value=MagicMock(),
            ) as mock_build,
        ):
            mock_settings.GOOGLE_PLAY_CREDENTIALS_PATH = ""
            mock_settings.GOOGLE_PLAY_CREDENTIALS_JSON = json_str
            mock_settings.GOOGLE_PLAY_PACKAGE_NAME = "com.skeddy.driver"

            svc = GooglePlayService()

            mock_from_info.assert_called_once_with(
                {"type": "service_account", "project_id": "test"},
                scopes=SCOPES,
            )
            mock_build.assert_called_once_with("androidpublisher", "v3", credentials=mock_creds)
            assert svc._package_name == "com.skeddy.driver"


# ---------------------------------------------------------------------------
# Test 3: File path takes priority over JSON string
# ---------------------------------------------------------------------------


class TestFilePathPriority:
    """When both env vars are set, file path takes priority."""

    def test_file_path_preferred_over_json(self):
        mock_creds = MagicMock()

        with (
            patch("app.services.google_play_service.settings") as mock_settings,
            patch(
                "app.services.google_play_service.service_account.Credentials"
                ".from_service_account_file",
                return_value=mock_creds,
            ) as mock_from_file,
            patch(
                "app.services.google_play_service.service_account.Credentials"
                ".from_service_account_info",
            ) as mock_from_info,
            patch(
                "app.services.google_play_service.build",
                return_value=MagicMock(),
            ),
        ):
            mock_settings.GOOGLE_PLAY_CREDENTIALS_PATH = "/path/to/creds.json"
            mock_settings.GOOGLE_PLAY_CREDENTIALS_JSON = '{"type": "service_account"}'
            mock_settings.GOOGLE_PLAY_PACKAGE_NAME = "com.skeddy.driver"

            GooglePlayService()

            mock_from_file.assert_called_once()
            mock_from_info.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4: ValueError when neither credentials env var is set
# ---------------------------------------------------------------------------


class TestMissingCredentials:
    """GooglePlayService raises ValueError when no credentials configured."""

    def test_raises_without_credentials(self):
        with (
            patch("app.services.google_play_service.settings") as mock_settings,
        ):
            mock_settings.GOOGLE_PLAY_CREDENTIALS_PATH = ""
            mock_settings.GOOGLE_PLAY_CREDENTIALS_JSON = ""
            mock_settings.GOOGLE_PLAY_PACKAGE_NAME = "com.skeddy.driver"

            with pytest.raises(ValueError, match="Google Play credentials not configured"):
                GooglePlayService()


# ---------------------------------------------------------------------------
# Test 5: ValueError when GOOGLE_PLAY_PACKAGE_NAME is not set
# ---------------------------------------------------------------------------


class TestMissingPackageName:
    """GooglePlayService raises ValueError when package name is empty."""

    def test_raises_without_package_name(self):
        mock_creds = MagicMock()

        with (
            patch("app.services.google_play_service.settings") as mock_settings,
            patch(
                "app.services.google_play_service.service_account.Credentials"
                ".from_service_account_file",
                return_value=mock_creds,
            ),
        ):
            mock_settings.GOOGLE_PLAY_CREDENTIALS_PATH = "/path/to/creds.json"
            mock_settings.GOOGLE_PLAY_CREDENTIALS_JSON = ""
            mock_settings.GOOGLE_PLAY_PACKAGE_NAME = ""

            with pytest.raises(ValueError, match="GOOGLE_PLAY_PACKAGE_NAME is required"):
                GooglePlayService()


# ---------------------------------------------------------------------------
# Test 6: androidpublisher service stored as _service attribute
# ---------------------------------------------------------------------------


class TestServiceAttribute:
    """Verify _service holds the built androidpublisher resource."""

    def test_service_attribute_stored(self):
        mock_creds = MagicMock()
        mock_api_service = MagicMock()

        with (
            patch("app.services.google_play_service.settings") as mock_settings,
            patch(
                "app.services.google_play_service.service_account.Credentials"
                ".from_service_account_file",
                return_value=mock_creds,
            ),
            patch(
                "app.services.google_play_service.build",
                return_value=mock_api_service,
            ),
        ):
            mock_settings.GOOGLE_PLAY_CREDENTIALS_PATH = "/path/to/creds.json"
            mock_settings.GOOGLE_PLAY_CREDENTIALS_JSON = ""
            mock_settings.GOOGLE_PLAY_PACKAGE_NAME = "com.skeddy.driver"

            svc = GooglePlayService()

            assert svc._service is mock_api_service


# ---------------------------------------------------------------------------
# Helper: create GooglePlayService with a mocked API backend
# ---------------------------------------------------------------------------


def _create_service(mock_api: MagicMock | None = None) -> GooglePlayService:
    """Bypass __init__ and inject mock _service + _package_name."""
    svc = object.__new__(GooglePlayService)
    svc._service = mock_api or MagicMock()
    svc._package_name = "com.skeddy.driver"
    return svc


def _make_api_response(
    *,
    purchase_state: int = 0,
    consumption_state: int = 0,
    order_id: str = "GPA.1234-5678-9012-34567",
    acknowledgement_state: int = 0,
    purchase_time_millis: str = "1708700000000",
) -> dict:
    return {
        "purchaseState": purchase_state,
        "consumptionState": consumption_state,
        "orderId": order_id,
        "acknowledgementState": acknowledgement_state,
        "purchaseTimeMillis": purchase_time_millis,
    }


# ---------------------------------------------------------------------------
# Test 7: Valid purchase returns GooglePurchaseResult
# ---------------------------------------------------------------------------


class TestVerifyPurchaseValidPurchase:
    """verify_purchase returns a correct GooglePurchaseResult for a valid purchase."""

    async def test_valid_purchase_returns_result(self):
        mock_api = MagicMock()
        response = _make_api_response()
        mock_api.purchases().products().get.return_value.execute.return_value = response

        svc = _create_service(mock_api)
        result = await svc.verify_purchase("credits_10", "token-abc")

        assert isinstance(result, GooglePurchaseResult)
        assert result.order_id == "GPA.1234-5678-9012-34567"
        assert result.purchase_state == 0
        assert result.consumption_state == 0
        assert result.acknowledgement_state == 0
        assert result.purchase_time_millis == "1708700000000"
        assert result.already_consumed is False

        mock_api.purchases().products().get.assert_called_with(
            packageName="com.skeddy.driver",
            productId="credits_10",
            token="token-abc",
        )


# ---------------------------------------------------------------------------
# Test 8: Canceled purchase (purchaseState=1) raises error
# ---------------------------------------------------------------------------


class TestVerifyPurchaseCanceled:
    """verify_purchase raises GooglePlayVerificationError when purchaseState=1."""

    async def test_canceled_purchase_raises_error(self):
        mock_api = MagicMock()
        response = _make_api_response(purchase_state=1)
        mock_api.purchases().products().get.return_value.execute.return_value = response

        svc = _create_service(mock_api)

        with pytest.raises(GooglePlayVerificationError) as exc_info:
            await svc.verify_purchase("credits_10", "token-abc")

        assert exc_info.value.code == "PURCHASE_NOT_COMPLETED"
        assert "purchaseState=1" in exc_info.value.message


# ---------------------------------------------------------------------------
# Test 9: Pending purchase (purchaseState=2) raises error
# ---------------------------------------------------------------------------


class TestVerifyPurchasePending:
    """verify_purchase raises GooglePlayVerificationError when purchaseState=2."""

    async def test_pending_purchase_raises_error(self):
        mock_api = MagicMock()
        response = _make_api_response(purchase_state=2)
        mock_api.purchases().products().get.return_value.execute.return_value = response

        svc = _create_service(mock_api)

        with pytest.raises(GooglePlayVerificationError) as exc_info:
            await svc.verify_purchase("credits_10", "token-abc")

        assert exc_info.value.code == "PURCHASE_NOT_COMPLETED"
        assert "purchaseState=2" in exc_info.value.message


# ---------------------------------------------------------------------------
# Test 10: Already consumed purchase sets already_consumed=True
# ---------------------------------------------------------------------------


class TestVerifyPurchaseAlreadyConsumed:
    """verify_purchase sets already_consumed=True when consumptionState=1."""

    async def test_already_consumed_sets_flag(self):
        mock_api = MagicMock()
        response = _make_api_response(consumption_state=1)
        mock_api.purchases().products().get.return_value.execute.return_value = response

        svc = _create_service(mock_api)
        result = await svc.verify_purchase("credits_10", "token-abc")

        assert result.already_consumed is True
        assert result.consumption_state == 1


# ---------------------------------------------------------------------------
# Test 11: Not consumed purchase sets already_consumed=False
# ---------------------------------------------------------------------------


class TestVerifyPurchaseNotConsumed:
    """verify_purchase sets already_consumed=False when consumptionState=0."""

    async def test_not_consumed_clears_flag(self):
        mock_api = MagicMock()
        response = _make_api_response(consumption_state=0)
        mock_api.purchases().products().get.return_value.execute.return_value = response

        svc = _create_service(mock_api)
        result = await svc.verify_purchase("credits_10", "token-abc")

        assert result.already_consumed is False
        assert result.consumption_state == 0


# ---------------------------------------------------------------------------
# Test 12: HttpError 404 raises GooglePlayVerificationError
# ---------------------------------------------------------------------------


class TestVerifyPurchaseHttpError404:
    """verify_purchase raises GooglePlayVerificationError for HTTP 404."""

    async def test_http_404_raises_invalid_token(self):
        mock_api = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 404
        http_error = HttpError(resp=mock_resp, content=b"Not found")
        mock_api.purchases().products().get.return_value.execute.side_effect = http_error

        svc = _create_service(mock_api)

        with pytest.raises(GooglePlayVerificationError) as exc_info:
            await svc.verify_purchase("credits_10", "token-bad")

        assert exc_info.value.code == "INVALID_PURCHASE_TOKEN"
        assert exc_info.value.__cause__ is http_error


# ---------------------------------------------------------------------------
# Test 12b: Non-404 HttpError is re-raised as-is
# ---------------------------------------------------------------------------


class TestVerifyPurchaseHttpErrorNon404:
    """verify_purchase re-raises non-404 HttpError without wrapping."""

    async def test_http_500_reraises(self):
        mock_api = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 500
        http_error = HttpError(resp=mock_resp, content=b"Internal server error")
        mock_api.purchases().products().get.return_value.execute.side_effect = http_error

        svc = _create_service(mock_api)

        with pytest.raises(HttpError) as exc_info:
            await svc.verify_purchase("credits_10", "token-abc")

        assert exc_info.value is http_error


# ---------------------------------------------------------------------------
# Test 13: verify_purchase delegates to run_in_executor
# ---------------------------------------------------------------------------


class TestVerifyPurchaseUsesExecutor:
    """verify_purchase calls run_in_executor with the default thread pool."""

    async def test_uses_run_in_executor(self):
        mock_api = MagicMock()
        response = _make_api_response()
        mock_execute = mock_api.purchases().products().get.return_value.execute

        mock_loop = MagicMock()
        mock_loop.run_in_executor = AsyncMock(return_value=response)

        svc = _create_service(mock_api)

        with patch(
            "app.services.google_play_service.asyncio.get_running_loop",
            return_value=mock_loop,
        ):
            await svc.verify_purchase("credits_10", "token-abc")

        mock_loop.run_in_executor.assert_called_once()
        call_args = mock_loop.run_in_executor.call_args
        assert call_args[0][0] is None  # default executor
        assert call_args[0][1] is mock_execute  # the .execute method


# ---------------------------------------------------------------------------
# consume_purchase tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 14: Successful consume returns True
# ---------------------------------------------------------------------------


class TestConsumePurchaseSuccess:
    """consume_purchase returns True when Google API consume succeeds."""

    async def test_successful_consume_returns_true(self):
        mock_api = MagicMock()
        # Google Play consume returns empty string on success
        mock_api.purchases().products().consume.return_value.execute.return_value = ""

        svc = _create_service(mock_api)
        result = await svc.consume_purchase("credits_10", "token-abc")

        assert result is True
        mock_api.purchases().products().consume.assert_called_with(
            packageName="com.skeddy.driver",
            productId="credits_10",
            token="token-abc",
        )


# ---------------------------------------------------------------------------
# Test 15: HttpError 404 returns False
# ---------------------------------------------------------------------------


class TestConsumePurchaseHttpError404:
    """consume_purchase returns False on HTTP 404 (already consumed or invalid)."""

    async def test_http_404_returns_false(self):
        mock_api = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 404
        http_error = HttpError(resp=mock_resp, content=b"Not found")
        mock_api.purchases().products().consume.return_value.execute.side_effect = http_error

        svc = _create_service(mock_api)
        result = await svc.consume_purchase("credits_10", "token-bad")

        assert result is False


# ---------------------------------------------------------------------------
# Test 16: HttpError 400 returns False
# ---------------------------------------------------------------------------


class TestConsumePurchaseHttpError400:
    """consume_purchase returns False on HTTP 400 (bad request)."""

    async def test_http_400_returns_false(self):
        mock_api = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 400
        http_error = HttpError(resp=mock_resp, content=b"Bad request")
        mock_api.purchases().products().consume.return_value.execute.side_effect = http_error

        svc = _create_service(mock_api)
        result = await svc.consume_purchase("credits_10", "token-abc")

        assert result is False


# ---------------------------------------------------------------------------
# Test 17: HttpError 503 returns False (no retry, caller handles)
# ---------------------------------------------------------------------------


class TestConsumePurchaseHttpError503:
    """consume_purchase returns False on HTTP 503 (service unavailable)."""

    async def test_http_503_returns_false(self):
        mock_api = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 503
        http_error = HttpError(resp=mock_resp, content=b"Service unavailable")
        mock_api.purchases().products().consume.return_value.execute.side_effect = http_error

        svc = _create_service(mock_api)
        result = await svc.consume_purchase("credits_10", "token-abc")

        assert result is False


# ---------------------------------------------------------------------------
# Test 18: consume_purchase logs on success and failure
# ---------------------------------------------------------------------------


class TestConsumePurchaseLogging:
    """consume_purchase logs info on success, error on failure."""

    async def test_logs_info_on_success(self):
        mock_api = MagicMock()
        mock_api.purchases().products().consume.return_value.execute.return_value = ""

        svc = _create_service(mock_api)

        with patch("app.services.google_play_service.logger") as mock_logger:
            await svc.consume_purchase("credits_10", "token-abc")

            mock_logger.info.assert_called_once()
            log_msg = mock_logger.info.call_args[0][0]
            assert "consumed" in log_msg.lower()

    async def test_logs_error_on_failure(self):
        mock_api = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status = 503
        http_error = HttpError(resp=mock_resp, content=b"Service unavailable")
        mock_api.purchases().products().consume.return_value.execute.side_effect = http_error

        svc = _create_service(mock_api)

        with patch("app.services.google_play_service.logger") as mock_logger:
            await svc.consume_purchase("credits_10", "token-abc")

            mock_logger.error.assert_called_once()
            log_msg = mock_logger.error.call_args[0][0]
            assert "consume failed" in log_msg.lower()
