import logging
import sys
import structlog
from structlog.types import EventDict, Processor

def configure_logging(app_env: str = "development") -> None:
    # Clear existing handlers
    logging.root.handlers = []

    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if app_env == "production":
        # JSON logs in production
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Human-readable color logs in development
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        logger_factory=structlog.PrintLoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )

logger = structlog.get_logger("distributed_job_platform")
