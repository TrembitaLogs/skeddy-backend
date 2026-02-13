import logging
import sys

from pythonjsonlogger.json import JsonFormatter

from app.middleware.request_id import request_id_ctx


class RequestIdFilter(logging.Filter):
    """Inject request_id from context into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_ctx.get()
        return True


def setup_logging(*, debug: bool = False) -> None:
    """Configure root logger with JSON formatter and request_id injection."""
    root = logging.getLogger()

    # Avoid duplicate handlers on repeated calls
    if any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        return

    handler = logging.StreamHandler(sys.stdout)
    formatter = JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
        defaults={"request_id": None},
    )
    handler.setFormatter(formatter)
    handler.addFilter(RequestIdFilter())

    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if debug else logging.INFO)
