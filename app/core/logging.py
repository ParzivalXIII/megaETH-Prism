import json
import logging
import sys
import uuid

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars


def setup_logging(log_level: str = "INFO") -> None:
    """Configure structlog for JSON-formatted output.

    Call once at application startup. Log level defaults to INFO.
    Use DEBUG for local development, WARNING/ERROR for production.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(serializer=json.dumps),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a bound logger with the given name."""
    return structlog.get_logger(name)


def bind_correlation_id() -> str:
    """Generate and bind a new UUID correlation ID to the current logging context.

    Returns the correlation ID string so it can be passed to downstream operations.
    """
    correlation_id = str(uuid.uuid4())
    bind_contextvars(correlation_id=correlation_id)
    return correlation_id


def reset_logging_context() -> None:
    """Clear all bound context variables."""
    clear_contextvars()


__all__ = [
    "setup_logging",
    "get_logger",
    "bind_correlation_id",
    "reset_logging_context",
]
