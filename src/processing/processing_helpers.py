from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from src.common.db import get_connection
from src.common.observability import ProcessingLogger

logger = logging.getLogger(__name__)


def _start_processing_run(conn, run_type: str, file_id: int, file_path: str | Path = "unknown") -> int:
    started_at = datetime.now().isoformat(timespec="seconds")

    run_cursor = conn.execute(
        """
        INSERT INTO processing_runs (
            run_type, file_id, started_at, run_status
        )
        VALUES (?, ?, ?, ?)
        """,
        (run_type, file_id, started_at, "RUNNING"),
    )
    run_id = run_cursor.lastrowid

    ProcessingLogger.log_run_start(run_id, file_id, run_type, str(file_path))

    return run_id


def _finalize_processing_run_success(conn, run_id: int, file_id: int, run_type: str, rows_read: int, rows_passed: int, rows_failed: int, issue_count: int):
    completed_at = datetime.now().isoformat(timespec="seconds")
    run_status = "SUCCESS" if rows_failed == 0 else "PARTIAL_SUCCESS"

    conn.execute(
        """
        UPDATE processing_runs
        SET completed_at = ?, run_status = ?, rows_read = ?, rows_passed = ?, rows_failed = ?, issue_count = ?
        WHERE run_id = ?
        """,
        (
            completed_at,
            run_status,
            rows_read,
            rows_passed,
            rows_failed,
            issue_count,
            run_id,
        ),
    )

    ProcessingLogger.log_run_complete(
        ProcessingLogger.create_metrics_from_finalize(
            run_id, file_id, run_type, rows_read, rows_passed, rows_failed, issue_count
        ),
        run_status
    )

    logger.info(
        "Finalized processing run success: run_id=%s file_id=%s status=%s rows_read=%s rows_passed=%s rows_failed=%s issues=%s",
        run_id,
        file_id,
        run_status,
        rows_read,
        rows_passed,
        rows_failed,
        issue_count,
    )


def _finalize_processing_run_failure(conn, run_id: int, rows_read: int, rows_passed: int, rows_failed: int, issue_count: int, exc: Exception):
    completed_at = datetime.now().isoformat(timespec="seconds")

    conn.execute(
        """
        UPDATE processing_runs
        SET completed_at = ?, run_status = ?, rows_read = ?, rows_passed = ?, rows_failed = ?, issue_count = ?, notes = ?
        WHERE run_id = ?
        """,
        (
            completed_at,
            "FAILED",
            rows_read,
            rows_passed,
            rows_failed,
            issue_count,
            str(exc),
            run_id,
        ),
    )

    logger.exception(
        "Finalized processing run failure: run_id=%s error=%s",
        run_id,
        exc,
    )


def _update_inbound_file_status(conn, file_id: int, status: str, error_count: int = 0):
    if status == "PROCESSED":
        conn.execute(
            """
            UPDATE inbound_files
            SET processing_status = ?, error_count = error_count + ?
            WHERE file_id = ?
            """,
            (status, error_count, file_id),
        )
    elif status == "FAILED":
        conn.execute(
            """
            UPDATE inbound_files
            SET processing_status = ?
            WHERE file_id = ?
            """,
            (status, file_id),
        )
    else:
        raise ValueError(f"Invalid status for file update: {status}")

    logger.info("Updated inbound file status: file_id=%s status=%s error_count=%s", file_id, status, error_count)


def _insert_audit_log(conn, event_type: str, entity_name: str, entity_key: str, run_id: int | None, file_id: int | None, event_details: str):
    conn.execute(
        """
        INSERT INTO audit_log (
            event_type, entity_name, entity_key, run_id, file_id, actor, event_details
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_type,
            entity_name,
            entity_key,
            run_id,
            file_id,
            "system",
            event_details,
        ),
    )

    logger.info("Inserted audit log: event_type=%s entity=%s key=%s run_id=%s file_id=%s", event_type, entity_name, entity_key, run_id, file_id)


def finalize_file_failure_durably(
    db_path: str | Path | None,
    run_id: int,
    file_id: int,
    rows_read: int,
    rows_passed: int,
    rows_failed: int,
    issue_count: int,
    exc: Exception,
) -> None:
    conn = get_connection(db_path)
    try:
        _finalize_processing_run_failure(
            conn=conn,
            run_id=run_id,
            rows_read=rows_read,
            rows_passed=rows_passed,
            rows_failed=rows_failed,
            issue_count=issue_count,
            exc=exc,
        )
        _update_inbound_file_status(conn, file_id, "FAILED")
        conn.commit()

        logger.exception(
            "Durably finalized file failure: run_id=%s file_id=%s error=%s",
            run_id,
            file_id,
            exc,
        )
    finally:
        conn.close()