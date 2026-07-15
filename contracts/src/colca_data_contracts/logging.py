"""Shared logging setup for Colca microservices.

Provides a consistent log format, secret sanitization, per-service log level
control via environment variables, and a SIGUSR1 toggle for runtime debug.

Usage in any service::

    from colca_data_contracts.logging import setup_logging
    logger = setup_logging(NAME)
"""

import logging
import re
import signal as _signal
import sys

from decouple import config

COLCA_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

_SECRET_PATTERNS = [
    (re.compile(r"(PWD|PASSWORD|password|passwd)=([^;\s&\"']+)", re.I), r"\1=***"),
    (re.compile(r"(token|api.key|secret|credential)([\"\\s:=]+)([^;\s&\"'}{]+)", re.I), r"\1\2***"),
    (re.compile(r"://([^:]+):([^@]+)@"), r"://\1:***@"),
]


def sanitize(msg: str) -> str:
    """Redact common secret patterns from a log message string."""
    for pattern, replacement in _SECRET_PATTERNS:
        msg = pattern.sub(replacement, msg)
    return msg


class SanitizingFormatter(logging.Formatter):
    """Formatter that runs secret sanitization on every log record."""

    def format(self, record):
        return sanitize(super().format(record))


def setup_logging(name: str, default_level: str = "INFO") -> logging.Logger:
    """Configure the root logger with Colca conventions and return a named logger.

    Log level resolution order:
      1. ``LOG_LEVEL_{NAME}`` env var (per-service override)
      2. ``LOG_LEVEL`` env var (global)
      3. *default_level* argument

    On Linux/macOS, SIGUSR1 toggles between the configured level and DEBUG.
    """
    env_key = f"LOG_LEVEL_{name.upper().replace('-', '_')}"
    level_str = config(env_key, default=config("LOG_LEVEL", default=default_level))
    level = getattr(logging, level_str.upper(), logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(SanitizingFormatter(COLCA_LOG_FORMAT))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Suppress noisy third-party loggers
    for noisy in ("urllib3", "paho", "asyncua", "httpcore", "httpx", "mcp"):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))

    # SIGUSR1 toggles DEBUG (Linux/macOS containers only)
    if sys.platform != "win32":
        _orig = level

        def _toggle(signum, frame):
            nonlocal _orig
            r = logging.getLogger()
            if r.level == logging.DEBUG:
                r.setLevel(_orig)
                r.handlers[0].emit(
                    logging.LogRecord(
                        name, logging.INFO, "", 0,
                        "[CONFIG] Log level restored to %s", (logging.getLevelName(_orig),), None,
                    )
                )
            else:
                r.setLevel(logging.DEBUG)
                r.handlers[0].emit(
                    logging.LogRecord(
                        name, logging.INFO, "", 0,
                        "[CONFIG] Log level switched to DEBUG (SIGUSR1 to restore)", (), None,
                    )
                )

        _signal.signal(_signal.SIGUSR1, _toggle)

    return logging.getLogger(name)
