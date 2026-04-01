import json
import logging
from io import StringIO

from pythonjsonlogger.json import JsonFormatter

from app.middleware.logging import RequestContextFilter, setup_logging
from app.middleware.request_id import request_id_ctx


class TestSetupLogging:
    """Tests for setup_logging() configuration."""

    def setup_method(self):
        """Reset root logger before each test."""
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def teardown_method(self):
        """Restore root logger after each test."""
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_logs_output_in_json_format(self):
        """Logs must be valid JSON with timestamp, level, message fields."""
        setup_logging(debug=False)

        stream = StringIO()
        root = logging.getLogger()
        root.handlers[0].stream = stream

        logging.getLogger("test.json").info("hello world")
        output = stream.getvalue().strip()

        record = json.loads(output)
        assert record["message"] == "hello world"
        assert record["level"] == "INFO"
        assert "timestamp" in record
        assert record["name"] == "test.json"

    def test_log_level_info_when_debug_false(self):
        """Root logger level should be INFO when debug=False."""
        setup_logging(debug=False)
        assert logging.getLogger().level == logging.INFO

    def test_log_level_debug_when_debug_true(self):
        """Root logger level should be DEBUG when debug=True."""
        setup_logging(debug=True)
        assert logging.getLogger().level == logging.DEBUG

    def test_debug_messages_visible_when_debug_true(self):
        """DEBUG-level messages should appear in output when debug=True."""
        setup_logging(debug=True)

        stream = StringIO()
        root = logging.getLogger()
        root.handlers[0].stream = stream

        logging.getLogger("test.debug").debug("debug msg")
        output = stream.getvalue().strip()

        record = json.loads(output)
        assert record["level"] == "DEBUG"
        assert record["message"] == "debug msg"

    def test_debug_messages_hidden_when_debug_false(self):
        """DEBUG-level messages should NOT appear when debug=False."""
        setup_logging(debug=False)

        stream = StringIO()
        root = logging.getLogger()
        root.handlers[0].stream = stream

        logging.getLogger("test.nodebug").debug("should not appear")
        output = stream.getvalue()
        assert output == ""

    def test_no_duplicate_handlers_on_repeated_calls(self):
        """Calling setup_logging() twice should not add duplicate handlers."""
        setup_logging(debug=False)
        setup_logging(debug=False)
        assert len(logging.getLogger().handlers) == 1

    def test_handler_uses_json_formatter(self):
        """The handler must use JsonFormatter."""
        setup_logging(debug=False)
        handler = logging.getLogger().handlers[0]
        assert isinstance(handler.formatter, JsonFormatter)


class TestRequestIdInLogs:
    """Tests for request_id propagation into log records."""

    def setup_method(self):
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def teardown_method(self):
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_request_id_present_when_context_set(self):
        """Logs should contain request_id when context variable is set."""
        setup_logging(debug=False)

        stream = StringIO()
        root = logging.getLogger()
        root.handlers[0].stream = stream

        token = request_id_ctx.set("test-req-123")
        try:
            logging.getLogger("test.reqid").info("with request id")
        finally:
            request_id_ctx.reset(token)

        record = json.loads(stream.getvalue().strip())
        assert record["request_id"] == "test-req-123"

    def test_request_id_null_when_context_not_set(self):
        """Logs should have request_id=null when no HTTP context exists."""
        setup_logging(debug=False)

        stream = StringIO()
        root = logging.getLogger()
        root.handlers[0].stream = stream

        logging.getLogger("test.noreqid").info("no request context")

        record = json.loads(stream.getvalue().strip())
        assert record["request_id"] is None


class TestRequestContextFilter:
    """Tests for the RequestContextFilter logging filter."""

    def test_filter_injects_request_id(self):
        """Filter should add request_id attribute to LogRecord."""
        f = RequestContextFilter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

        token = request_id_ctx.set("filter-test-id")
        try:
            result = f.filter(record)
        finally:
            request_id_ctx.reset(token)

        assert result is True
        assert record.request_id == "filter-test-id"  # type: ignore[attr-defined]

    def test_filter_returns_none_without_context(self):
        """Filter should set request_id=None when context not set."""
        f = RequestContextFilter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

        result = f.filter(record)

        assert result is True
        assert record.request_id is None  # type: ignore[attr-defined]
