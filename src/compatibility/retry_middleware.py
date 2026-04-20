"""
CockroachDB transaction retry middleware with exponential backoff and jitter.

Handles SERIALIZABLE isolation retry errors (SQLSTATE 40001) which are expected
when CockroachDB detects transaction contention. This is the recommended pattern
for CockroachDB — use SERIALIZABLE isolation (the default, strongest guarantee)
with automatic retry on contention, rather than weakening to READ COMMITTED.

Pattern based on:
  https://github.com/cockroachdb/langchain-cockroachdb/blob/main/langchain_cockroachdb/retry.py

Usage (sync):
    @sync_retry_with_backoff()
    def do_work(session):
        session.execute(text("INSERT INTO ..."))
        session.commit()

Usage (async):
    @async_retry_with_backoff()
    async def do_work(session):
        await session.execute(text("INSERT INTO ..."))
        await session.commit()

Usage (SQLAlchemy engine event):
    register_retry_listener(engine)
"""

import asyncio
import functools
import logging
import random
import time

logger = logging.getLogger(__name__)

# Default retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 0.05  # 50ms
MAX_BACKOFF = 5.0  # 5 seconds
BACKOFF_MULTIPLIER = 2.0
JITTER = True  # Apply random jitter to avoid thundering herd


def is_retryable_error(error: Exception) -> bool:
    """
    Check if an exception is a transient CockroachDB error that can be retried.

    Detects:
      - SQLSTATE 40001 (serialization_failure / retry_write_too_old)
      - Connection-level transient errors (reset, broken pipe)
    """
    # Check pgcode attribute (psycopg2 / asyncpg)
    pgcode = getattr(error, "pgcode", None)
    if pgcode == "40001":
        return True

    # Check wrapped / chained exceptions
    cause = getattr(error, "__cause__", None) or getattr(error, "orig", None)
    if cause is not None:
        cause_pgcode = getattr(cause, "pgcode", None)
        if cause_pgcode == "40001":
            return True

    # Fall back to string matching for wrapped errors
    error_str = str(error).lower()
    retryable_patterns = [
        "restart transaction",
        "40001",
        "serialization failure",
        "retry_write_too_old",
        "connection reset",
        "broken pipe",
        "connection refused",
    ]
    return any(pattern in error_str for pattern in retryable_patterns)


def _calculate_delay(
    attempt: int,
    initial_backoff: float = INITIAL_BACKOFF,
    max_backoff: float = MAX_BACKOFF,
    backoff_multiplier: float = BACKOFF_MULTIPLIER,
    jitter: bool = JITTER,
) -> float:
    """Calculate exponential backoff delay with optional jitter."""
    delay = min(initial_backoff * (backoff_multiplier**attempt), max_backoff)
    if jitter:
        # Jitter between 50% and 100% of the calculated delay
        delay = delay * random.uniform(0.5, 1.0)
    return delay


def sync_retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
    max_backoff: float = MAX_BACKOFF,
    backoff_multiplier: float = BACKOFF_MULTIPLIER,
    jitter: bool = JITTER,
):
    """
    Decorator that retries a synchronous function on transient CockroachDB errors.

    Uses exponential backoff with jitter to avoid thundering herd.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if not is_retryable_error(e) or attempt == max_retries:
                        raise
                    delay = _calculate_delay(
                        attempt, initial_backoff, max_backoff,
                        backoff_multiplier, jitter,
                    )
                    logger.warning(
                        "CockroachDB retryable error on attempt %d/%d, "
                        "retrying in %.3fs: %s",
                        attempt + 1,
                        max_retries,
                        delay,
                        e,
                    )
                    time.sleep(delay)
            raise last_error  # Should not reach here, but safety net

        return wrapper

    return decorator


def async_retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
    max_backoff: float = MAX_BACKOFF,
    backoff_multiplier: float = BACKOFF_MULTIPLIER,
    jitter: bool = JITTER,
):
    """
    Decorator that retries an async function on transient CockroachDB errors.

    Uses exponential backoff with jitter to avoid thundering herd.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if not is_retryable_error(e) or attempt == max_retries:
                        raise
                    delay = _calculate_delay(
                        attempt, initial_backoff, max_backoff,
                        backoff_multiplier, jitter,
                    )
                    logger.warning(
                        "CockroachDB retryable error on attempt %d/%d, "
                        "retrying in %.3fs: %s",
                        attempt + 1,
                        max_retries,
                        delay,
                        e,
                    )
                    await asyncio.sleep(delay)
            raise last_error  # Should not reach here, but safety net

        return wrapper

    return decorator


def run_with_retry(session_factory, fn, max_retries=MAX_RETRIES):
    """
    Execute a function with automatic retry on serialization failure.

    Creates a fresh session for each attempt and handles rollback/close.

    Args:
        session_factory: Callable that creates a new SQLAlchemy session
        fn: Callable(session) -> result. Will be retried on 40001 errors.
        max_retries: Maximum number of retry attempts

    Returns:
        The return value of fn(session)

    Example:
        def do_work(session):
            session.execute(text("INSERT INTO ..."))
            session.commit()
            return "done"

        result = run_with_retry(Session, do_work)
    """
    for attempt in range(max_retries + 1):
        session = session_factory()
        try:
            result = fn(session)
            return result
        except Exception as e:
            session.rollback()
            if is_retryable_error(e) and attempt < max_retries:
                delay = _calculate_delay(attempt)
                logger.warning(
                    "Serialization error on attempt %d/%d, retrying in %.3fs: %s",
                    attempt + 1,
                    max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)
            else:
                raise
        finally:
            session.close()


def register_retry_listener(engine):
    """
    Register a SQLAlchemy event listener that logs CockroachDB retry errors.

    Note: SQLAlchemy's handle_error event cannot transparently retry
    transactions. This listener logs diagnostic information. For actual
    retries, use run_with_retry() or the decorator functions above.
    """
    from sqlalchemy import event

    @event.listens_for(engine, "handle_error")
    def handle_serialization_error(context):
        if not is_retryable_error(context.original_exception):
            return
        logger.warning(
            "CockroachDB serialization error detected (40001). "
            "Wrap your transaction with run_with_retry() or "
            "use @sync_retry_with_backoff() for automatic retries. "
            "Error: %s",
            context.original_exception,
        )
