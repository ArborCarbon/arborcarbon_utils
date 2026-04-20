"""Base Postgres helpers shared across ArborCarbon services."""

import contextlib
import logging
import time
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import psycopg
from psycopg.rows import dict_row

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from types import TracebackType

    from psycopg import Connection, Transaction
    from psycopg.abc import Params
    from psycopg.sql import SQL

##################################################################################################
# Types
##################################################################################################
Row = dict[str, Any]
_CONNECTION_CLEANUP_ERRORS = (psycopg.Error, OSError, RuntimeError)

# How long to wait for a TCP connection to Postgres before giving up per attempt.
_CONNECT_TIMEOUT_S = 5

# Delays *before* each successive connect attempt (first attempt is immediate).
# Total worst-case wait before raising: sum of all delays = 0 + 0.5 + 1.5 = 2.0 s.
_CONNECT_RETRY_DELAYS_S: tuple[float, ...] = (0.0, 0.5, 1.5)


##################################################################################################
# Enums
##################################################################################################
class DBReturnType(StrEnum):
    """Controls the shape of rows returned from _execute."""

    ALL = "all"
    ONE = "one"
    VAL = "val"
    VALS = "vals"
    NONE = "none"


##################################################################################################
# Classes
##################################################################################################
class BaseDatabase:
    """
    Thin psycopg-backed base class providing connection management and query execution.

    Subclass this and add domain-specific query methods on top.
    """

    ##############################################################################################
    # Constructors
    ##############################################################################################
    def __init__(self, dsn: str):
        """Store the DSN and initialise the optional shared connection."""
        self.dsn = dsn
        self._connection: Connection | None = None
        self._transaction: AbstractContextManager[Transaction] | None = None

    ##############################################################################################
    # Built-in methods
    ##############################################################################################
    def __enter__(self):
        """Open and retain a shared connection for use inside a with block."""
        self._connection = self._connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        """Close the retained context-managed connection when leaving the with block."""
        if self._connection:
            self._connection.close()
            self._connection = None

    ##############################################################################################
    # Attributes
    ##############################################################################################
    @property
    def connection(self) -> Connection:
        """Return the active shared connection, or open a temporary connection on demand."""
        return self._connection or self._connect()

    ##############################################################################################
    # Public query methods
    ##############################################################################################
    def fetchall(
        self,
        sql: str,
        params: Params | None = None,
    ) -> list[Row]:
        """Execute a query and return all rows as dictionaries."""

        def _query(conn: Connection) -> list[Row]:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(cast("SQL", sql), params or ())
                return list(cur.fetchall())

        return self._run_query(_query)

    ##############################################################################################
    # Internal DB methods
    ##############################################################################################
    def _connect(self) -> Connection:
        """
        Open a new psycopg connection, retrying briefly on transient failures.

        ``OperationalError`` covers "connection refused", "server not available",
        and similar network-level problems that are often transient in a
        containerised environment.  Each attempt enforces a hard TCP timeout so
        the call never hangs indefinitely on an unreachable host.
        """
        *retry_delays, final_delay = _CONNECT_RETRY_DELAYS_S
        total = len(_CONNECT_RETRY_DELAYS_S)

        for attempt, delay in enumerate(retry_delays):
            if delay > 0:
                time.sleep(delay)
            try:
                return psycopg.connect(self.dsn, connect_timeout=_CONNECT_TIMEOUT_S)
            except psycopg.OperationalError as exc:
                logging.warning(
                    "DB connect failed (attempt %d/%d), retrying: %s",
                    attempt + 1,
                    total,
                    exc,
                )

        # Final attempt — any exception propagates to the caller.
        if final_delay > 0:
            time.sleep(final_delay)
        return psycopg.connect(self.dsn, connect_timeout=_CONNECT_TIMEOUT_S)

    @overload
    def _execute(
        self,
        sql: str,
        params: Params | None = None,
        *,
        return_val: None = None,
    ): ...

    @overload
    def _execute(
        self,
        sql: str,
        params: Params | None = None,
        *,
        return_val: Literal[DBReturnType.NONE],
    ): ...

    @overload
    def _execute(
        self,
        sql: str,
        params: Params | None = None,
        *,
        return_val: Literal[DBReturnType.ONE],
    ) -> Row | None: ...

    @overload
    def _execute(
        self,
        sql: str,
        params: Params | None = None,
        *,
        return_val: Literal[DBReturnType.ALL],
    ) -> list[Row]: ...

    @overload
    def _execute(
        self,
        sql: str,
        params: Params | None = None,
        *,
        return_val: Literal[DBReturnType.VAL],
    ) -> object | None: ...

    @overload
    def _execute(
        self,
        sql: str,
        params: Params | None = None,
        *,
        return_val: Literal[DBReturnType.VALS],
    ) -> list[Any]: ...

    def _execute(
        self,
        sql: str,
        params: Params | None = None,
        *,
        return_val: DBReturnType | None = None,
    ) -> Row | list[Row] | object | list[Any] | None:
        """Execute a statement and adapt the return shape to the requested DBReturnType."""
        if return_val is None:
            return_val = DBReturnType.NONE

        def _query(conn: Connection):
            ret: Row | list[Row] | Any | list[Any] | None = None
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(cast("SQL", sql), params or ())
                match return_val:
                    case DBReturnType.ONE:
                        ret = cur.fetchone()
                    case DBReturnType.ALL:
                        ret = cur.fetchall()
                    case DBReturnType.VAL:
                        row = cur.fetchone()
                        ret = None if row is None else next(iter(row.values()))
                    case DBReturnType.VALS:
                        ret = [next(iter(r.values())) for r in cur.fetchall()]
            return ret

        return self._run_query(_query)

    def _fetchone(
        self,
        sql: str,
        params: Params | None = None,
    ) -> Row | None:
        """Execute a query and return the first row as a dictionary, or None."""

        def _query(conn: Connection) -> Row | None:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(cast("SQL", sql), params or ())
                return cur.fetchone()

        return self._run_query(_query)

    def _run_query(self, fnc: Callable[[Connection]]):
        """Run a query callback with one retry for connection-level failures."""
        retryable = (psycopg.OperationalError, psycopg.InterfaceError)

        for attempt in range(2):
            conn = self.connection
            try:
                result = fnc(conn)
            except retryable:
                with contextlib.suppress(*_CONNECTION_CLEANUP_ERRORS):
                    conn.rollback()

                if self._connection is not None:
                    with contextlib.suppress(*_CONNECTION_CLEANUP_ERRORS):
                        self._connection.close()
                    self._connection = None

                with contextlib.suppress(*_CONNECTION_CLEANUP_ERRORS):
                    conn.close()

                if attempt == 0:
                    time.sleep(0.2)
                    continue
                raise
            except BaseException:
                with contextlib.suppress(*_CONNECTION_CLEANUP_ERRORS):
                    conn.rollback()
                raise
            else:
                conn.commit()
                return result
            finally:
                if self._connection is None:
                    with contextlib.suppress(*_CONNECTION_CLEANUP_ERRORS):
                        conn.close()
        error = "Unreachable: query retry loop exhausted"
        raise RuntimeError(error)
