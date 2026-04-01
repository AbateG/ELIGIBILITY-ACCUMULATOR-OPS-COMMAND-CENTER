"""
Eligibility Processing Pipeline

This module handles the processing of eligibility files through validation and insertion.

Processing includes:
1. File validation for schema and referential integrity
2. Duplicate and conflict detection
3. Issue creation for invalid rows
4. Eligibility period insertion for valid rows

Status lifecycles follow the same pattern as claims processing.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from src.common.db import db_session, fetch_all
from src.common.observability import ProcessingLogger, create_processing_metrics, finalize_processing_metrics
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.processing.processing_helpers import (
    _finalize_processing_run_success,
    _insert_audit_log,
    _start_processing_run,
    _update_inbound_file_status,
    finalize_file_failure_durably,
)
from src.sla.sla_service import evaluate_open_slas
from src.validation.eligibility_validators import (
    create_row_issues,
    validate_duplicate_eligibility_rows,
    validate_eligibility_period_conflicts,
    validate_eligibility_row_referential,
    validate_eligibility_row_schema,
)

logger = logging.getLogger(__name__)


def _normalize_string(value: Any) -> str:
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


def load_reference_data(conn) -> dict[str, Any]:
    client_rows = conn.execute("SELECT client_id, client_code FROM clients").fetchall()
    vendor_rows = conn.execute("SELECT vendor_id, vendor_code FROM vendors").fetchall()
    plan_rows = conn.execute("SELECT plan_id, plan_code FROM benefit_plans").fetchall()

    member_rows = conn.execute(
        """
        SELECT
            m.member_id,
            m.subscriber_id,
            m.relationship_code,
            c.client_code,
            m.client_id
        FROM members m
        JOIN clients c
          ON m.client_id = c.client_id
        """
    ).fetchall()

    return {
        "client_codes": {row["client_code"] for row in client_rows},
        "vendor_codes": {row["vendor_code"] for row in vendor_rows},
        "plan_codes": {row["plan_code"] for row in plan_rows},
        "client_id_by_code": {row["client_code"]: row["client_id"] for row in client_rows},
        "vendor_id_by_code": {row["vendor_code"]: row["vendor_id"] for row in vendor_rows},
        "plan_id_by_code": {row["plan_code"]: row["plan_id"] for row in plan_rows},
        "member_map": {
            row["member_id"]: {
                "subscriber_id": row["subscriber_id"],
                "relationship_code": row["relationship_code"],
                "client_code": row["client_code"],
                "client_id": row["client_id"],
            }
            for row in member_rows
        },
    }


def get_validated_eligibility_files(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM inbound_files
        WHERE file_type = 'ELIGIBILITY'
          AND processing_status = 'VALIDATED'
        ORDER BY file_id
        """,
        db_path=db_path
    )


def insert_eligibility_row(
    conn,
    row: pd.Series,
    file_id: int,
    row_number: int,
    ref_data: dict[str, Any],
) -> None:
    client_id = ref_data["client_id_by_code"][row["client_code"]]
    vendor_id = ref_data["vendor_id_by_code"].get(row["vendor_code"])
    plan_id = ref_data["plan_id_by_code"][row["plan_code"]]

    conn.execute(
        """
        INSERT INTO eligibility_periods (
            member_id,
            subscriber_id,
            client_id,
            plan_id,
            vendor_id,
            group_id,
            coverage_start,
            coverage_end,
            status,
            source_file_id,
            source_row_number
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["member_id"],
            row["subscriber_id"],
            client_id,
            plan_id,
            vendor_id,
            row.get("group_id"),
            row["coverage_start"],
            row["coverage_end"] if pd.notna(row.get("coverage_end")) else None,
            row["status"],
            file_id,
            row_number,
        ),
    )


def _read_eligibility_csv(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Eligibility file not found: {file_path}")

    if not file_path.is_file():
        raise FileNotFoundError(f"Eligibility path is not a file: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except EmptyDataError as exc:
        raise ValueError(f"Eligibility file is empty: {file_path}") from exc
    except ParserError as exc:
        raise ValueError(f"Eligibility file could not be parsed as CSV: {file_path}") from exc

    if df.empty:
        raise ValueError(f"Eligibility file contains no data rows: {file_path}")

    return df


def _resolve_client_vendor_ids(
    row: pd.Series,
    ref_data: dict[str, Any],
) -> tuple[int | None, int | None]:
    client_code = _normalize_string(row.get("client_code"))
    vendor_code = _normalize_string(row.get("vendor_code"))

    client_id = ref_data["client_id_by_code"].get(client_code) if client_code else None
    vendor_id = ref_data["vendor_id_by_code"].get(vendor_code) if vendor_code else None

    return client_id, vendor_id


def _perform_batch_validation(df: pd.DataFrame) -> dict[int, list[dict[str, str]]]:
    """Perform batch-level validations that require looking across all rows."""
    eligibility_rows = [row for _, row in df.iterrows()]
    batch_issues_by_row: dict[int, list[dict[str, str]]] = {}

    duplicate_issue_results = validate_duplicate_eligibility_rows(eligibility_rows)
    conflict_issue_results = validate_eligibility_period_conflicts(eligibility_rows)

    for result in duplicate_issue_results + conflict_issue_results:
        batch_issues_by_row.setdefault(result["row_number"], []).append(result["issue"])

    return batch_issues_by_row


def _validate_and_process_eligibility_row(
    conn,
    row: pd.Series,
    row_number: int,
    file_id: int,
    run_id: int,
    ref_data: dict[str, Any],
    batch_issues_by_row: dict[int, list[dict[str, str]]],
) -> tuple[bool, int]:
    """Validate a single eligibility row and insert if valid. Returns (success, issue_count)."""
    schema_issues = validate_eligibility_row_schema(row)
    referential_issues = validate_eligibility_row_referential(row, ref_data)
    batch_issues = batch_issues_by_row.get(row_number, [])
    all_issues = schema_issues + referential_issues + batch_issues

    client_id, vendor_id = _resolve_client_vendor_ids(row, ref_data)

    if all_issues:
        issue_ids = create_row_issues(
            conn=conn,
            file_id=file_id,
            run_id=run_id,
            row_number=row_number,
            row=row,
            issues=all_issues,
            client_id=client_id,
            vendor_id=vendor_id,
            ref_data=ref_data,
        )
        return False, len(issue_ids)

    insert_eligibility_row(
        conn=conn,
        row=row,
        file_id=file_id,
        row_number=row_number,
        ref_data=ref_data,
    )
    return True, 0


def _finalize_eligibility_processing_success(
    conn,
    run_id: int,
    file_id: int,
    run_type: str,
    rows_read: int,
    rows_passed: int,
    rows_failed: int,
    total_issue_count: int,
) -> None:
    """Finalize successful eligibility processing."""
    _finalize_processing_run_success(conn, run_id, file_id, run_type, rows_read, rows_passed, rows_failed, total_issue_count)
    _update_inbound_file_status(conn, file_id, "PROCESSED", total_issue_count)
    _insert_audit_log(
        conn,
        "ELIGIBILITY_LOADED",
        "inbound_files",
        str(file_id),
        run_id,
        file_id,
        f"Eligibility processing complete: rows_read={rows_read}, rows_passed={rows_passed}, rows_failed={rows_failed}, issues={total_issue_count}",
    )


def _process_single_eligibility_file(
    conn,
    file_record: dict[str, Any],
    ref_data: dict[str, Any],
    db_path: str | Path | None,
) -> None:
    """Process a single eligibility file."""
    file_id = file_record["file_id"]
    file_path = Path(file_record["landing_path"])

    run_id = _start_processing_run(conn, "ELIGIBILITY_LOAD", file_id, file_path)

    # Initialize metrics
    metrics = create_processing_metrics(run_id, file_id, "ELIGIBILITY_LOAD", 0, 0, 0, 0)

    rows_read = 0
    rows_passed = 0
    rows_failed = 0
    total_issue_count = 0

    try:
        df = _read_eligibility_csv(file_path)
        rows_read = len(df)
        metrics.rows_read = rows_read

        batch_issues_by_row = _perform_batch_validation(df)

        ProcessingLogger.log_batch_validation_complete(
            run_id, file_id, rows_read, len(batch_issues_by_row)
        )

        for row_idx, row in df.iterrows():
            row_number = row_idx + 2  # account for header row in CSV

            success, issues_created = _validate_and_process_eligibility_row(
                conn=conn,
                row=row,
                row_number=row_number,
                file_id=file_id,
                run_id=run_id,
                ref_data=ref_data,
                batch_issues_by_row=batch_issues_by_row,
            )

            ProcessingLogger.log_row_processed(run_id, file_id, row_number, success, issues_created)

            if success:
                rows_passed += 1
            else:
                rows_failed += 1
                total_issue_count += issues_created

        # Update metrics
        metrics.rows_passed = rows_passed
        metrics.rows_failed = rows_failed
        metrics.issue_count = total_issue_count

        # Post-processing steps
        create_support_cases_from_open_issues(conn)
        evaluate_open_slas(conn)

        _finalize_eligibility_processing_success(
            conn, run_id, file_id, "ELIGIBILITY_LOAD", rows_read, rows_passed, rows_failed, total_issue_count
        )

        # Finalize and log metrics
        metrics = finalize_processing_metrics(metrics)
        ProcessingLogger.log_run_complete(metrics, "SUCCESS")

    except (FileNotFoundError, ValueError, KeyError, pd.errors.ParserError) as exc:
        # Update metrics for failed run
        metrics.rows_read = rows_read
        metrics.rows_passed = rows_passed
        metrics.rows_failed = rows_failed
        metrics.issue_count = total_issue_count
        metrics = finalize_processing_metrics(metrics)

        ProcessingLogger.log_run_error(run_id, file_id, exc)
        ProcessingLogger.log_run_complete(metrics, "FAILED")

        finalize_file_failure_durably(
            db_path=db_path,
            run_id=run_id,
            file_id=file_id,
            rows_read=rows_read,
            rows_passed=rows_passed,
            rows_failed=rows_failed,
            issue_count=total_issue_count,
            exc=exc,
        )
        raise


def process_eligibility_files(db_path: str | Path | None = None) -> None:
    """Process all validated eligibility files."""
    files = get_validated_eligibility_files(db_path=db_path)

    if not files:
        logger.info("No VALIDATED eligibility files available for processing.")
        return

    logger.info("Starting eligibility processing for %s file(s).", len(files))

    with db_session(db_path) as conn:
        ref_data = load_reference_data(conn)

        for file_record in files:
            _process_single_eligibility_file(conn, file_record, ref_data, db_path)

    logger.info("Processed %s eligibility file(s).", len(files))


if __name__ == "__main__":
    process_eligibility_files()