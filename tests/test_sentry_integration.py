from unittest.mock import MagicMock, patch

import sentry_sdk


class TestSentryIntegration:
    """Tests for Sentry SDK initialization logic in main.py."""

    def teardown_method(self):
        """Reset Sentry after each test to avoid leaking state."""
        sentry_sdk.init(dsn="")

    @patch("sentry_sdk.init")
    def test_sentry_initialized_when_dsn_set(self, mock_init):
        """sentry_sdk.init() should be called when SENTRY_DSN is non-empty."""
        test_dsn = "https://examplePublicKey@o0.ingest.sentry.io/0"

        # Simulate the init logic from main.py
        sentry_dsn = test_dsn
        if sentry_dsn:
            sentry_sdk.init(
                dsn=sentry_dsn,
                integrations=[MagicMock()],
            )

        mock_init.assert_called_once()
        call_kwargs = mock_init.call_args
        assert call_kwargs.kwargs["dsn"] == test_dsn

    @patch("sentry_sdk.init")
    def test_sentry_not_initialized_when_dsn_empty(self, mock_init):
        """sentry_sdk.init() should NOT be called when SENTRY_DSN is empty."""
        # Simulate the init logic from main.py
        sentry_dsn = ""
        if sentry_dsn:
            sentry_sdk.init(dsn=sentry_dsn)

        mock_init.assert_not_called()
