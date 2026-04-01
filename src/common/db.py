from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is always importable (safety net for sub-pages)
_repo_root = Path(__file__).resolve().parents[2]  # src/common/db.py → ../../ = repo root
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from config.settings import DB_PATH

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base exception for database-related failures."""


class DatabaseConnectionError(DatabaseError):
    """Raised when a database connection cannot be established."""


class DatabaseQueryError(DatabaseError):
    """Raised when a database query fails."""


class DatabaseTransactionError(DatabaseError):
    """Raised when a database transaction fails."""


def _resolve_db_path(db_path: str | Path | None = None) -> Path:
    if db_path is not None:
        return Path(db_path)

    env_path = os.environ.get("SQLITE_DB_PATH")
    if env_path:
        return Path(env_path)

    return Path(DB_PATH)


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    resolved = _resolve_db_path(db_path)

    try:
        conn = sqlite3.connect(str(resolved))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    except sqlite3.Error as exc:
        logger.exception("Failed to connect to database at path=%s", resolved)
        raise DatabaseConnectionError(f"Failed to connect to database: {resolved}") from exc


@contextmanager
def db_session(db_path: str | Path | None = None) -> Iterator[sqlite3.Connection]:
    resolved = _resolve_db_path(db_path)
    conn = get_connection(resolved)

    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except sqlite3.Error:
            logger.exception("Rollback failed during database transaction at path=%s", resolved)
        raise
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            logger.exception("Failed to close database connection at path=%s", resolved)


def fetch_all(
    query: str,
    params: tuple = (),
    db_path: str | Path | None = None,
) -> list[dict]:
    try:
        with db_session(db_path) as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as exc:
        logger.exception("fetch_all failed for query=%s params=%s", query, params)
        raise DatabaseQueryError(f"Failed to fetch rows: {exc}") from exc
    except DatabaseError:
        raise


def fetch_one(
    query: str,
    params: tuple = (),
    db_path: str | Path | None = None,
) -> dict | None:
    try:
        with db_session(db_path) as conn:
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as exc:
        logger.exception("fetch_one failed for query=%s params=%s", query, params)
        raise DatabaseQueryError(f"Failed to fetch row: {exc}") from exc
    except DatabaseError:
        raise


def execute(
    query: str,
    params: tuple = (),
    db_path: str | Path | None = None,
) -> None:
    try:
        with db_session(db_path) as conn:
            conn.execute(query, params)
    except sqlite3.Error as exc:
        logger.exception("execute failed for query=%s params=%s", query, params)
        raise DatabaseQueryError(f"Failed to execute query: {exc}") from exc
    except DatabaseError:
        raise


def executemany(
    query: str,
    params_list: list[tuple],
    db_path: str | Path | None = None,
) -> None:
    try:
        with db_session(db_path) as conn:
            conn.executemany(query, params_list)
    except sqlite3.Error as exc:
        logger.exception(
            "executemany failed for query=%s param_count=%s",
            query,
            len(params_list),
        )
        raise DatabaseQueryError(f"Failed to execute batch query: {exc}") from exc
    except DatabaseError:
        raise





def execute_insert(sql: str, params: tuple = ()) -> int:
    """Execute an INSERT and return the last row id."""
    try:
        with db_session() as conn:
            cursor = conn.execute(sql, params)
            return cursor.lastrowid
    except sqlite3.Error as exc:
        logger.exception("execute_insert failed for sql=%s params=%s", sql, params)
        raise DatabaseQueryError(f"Failed to execute insert: {exc}") from exc