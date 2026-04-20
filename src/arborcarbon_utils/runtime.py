"""Runtime helpers shared across ArborCarbon services."""

from __future__ import annotations

import logging
import signal
from typing import TYPE_CHECKING, TypeVar

import psycopg

if TYPE_CHECKING:
    from collections.abc import Callable

##################################################################################################
# Types
##################################################################################################
T = TypeVar("T")


##################################################################################################
# Constants
##################################################################################################
# Exceptions that are commonly transient in long-running services (DB/network/IO).
TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    psycopg.Error,
    ConnectionError,
    TimeoutError,
    OSError,
)


##################################################################################################
# Public methods
##################################################################################################
def best_effort(
    action: str,
    fn: Callable[[], T],
    *,
    default: T | None = None,
    exceptions: tuple[type[BaseException], ...] = TRANSIENT_EXCEPTIONS,
    log: Callable[[str], None] = logging.warning,
) -> T | None:
    """
    Run *fn* and return its result, but swallow common transient failures.

    Intended for background refreshes (e.g., reload config from DB) where failure
    should not crash the service.

    Args:
        action: Short description used in log messages (e.g., "tuning reload").
        fn: Callable to execute.
        default: Value returned when an exception is caught.
        exceptions: Exception types to catch.
        log: Logging callable used for warnings.

    Returns:
        fn() result or *default* if a caught exception occurs.
    """
    try:
        return fn()
    except exceptions as ex:  # - explicitly best-effort
        log(f"{action} failed: {ex}")
        return default


def signal_name(signum: int) -> str:
    """Best-effort conversion of a signal number to a readable name."""
    try:
        return signal.Signals(signum).name
    except (ValueError, TypeError):
        return str(signum)
