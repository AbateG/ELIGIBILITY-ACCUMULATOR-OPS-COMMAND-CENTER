"""
Claims Processing Pipeline

This module handles the processing of claims files through the following pipeline:

1. File Validation: Claims files are validated for schema, referential integrity, and business rules
2. Issue Creation: Invalid rows create data quality issues
3. Claim Insertion: Valid claims are inserted into the database
4. Transaction Derivation: Accumulator transactions are derived from claims
5. Snapshot Rebuild: Accumulator snapshots are rebuilt from transactions
6. Anomaly Detection: Snapshots are checked for consistency
7. Support Case Generation: Issues trigger support case creation
8. SLA Evaluation: Open SLAs are evaluated

File Status Lifecycle:
- RECEIVED: File ingested but not validated
- VALIDATED: File passed initial validation
- PROCESSED: File successfully processed (may have issues)
- FAILED: File processing failed

Processing Run Status Lifecycle:
- RUNNING: Processing in progress
- SUCCESS: All rows processed without issues
- PARTIAL_SUCCESS: Some rows processed, some failed
- FAILED: Processing failed due to errors
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from src.accumulators.snapshot_engine import detect_accumulator_anomalies, rebuild_accumulator_snapshots
from src.accumulators.transaction_engine import derive_accumulator_transactions
from src.common.db import db_session, fetch_all
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.sla.sla_service import evaluate_open_slas
from src.validation.claims_validators import (
    create_claim_row_issues,
    validate_claim_amount_relationships,
    validate_claim_row_referential_and_eligibility,
    validate_claim_row_schema,
    validate_duplicate_claim_rows,
)
from src.processing.processing_helpers import (
    _finalize_processing_run_success,
    _insert_audit_log,
    _start_processing_run,
    _update_inbound_file_status,
    finalize_file_failure_durably,
)

logger = logging.getLogger(__name__)


def _normalize_string(value: Any) -> str:
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


def _is_truthy_flag(value: Any) -> bool:
    return _normalize_string(value).upper() in {"1", "TRUE", "Y", "YES"}


def load_reference_data(conn) -> dict[str, Any]:
    client_rows = conn.execute("SELECT client_id, client_code FROM clients").fetchall()
    vendor_rows = conn.execute("SELECT vendor_id, vendor_code FROM vendors").fetchall()
    plan_rows = conn.execute(
        "SELECT plan_id, plan_code, benefit_year, preventive_exempt_flag FROM benefit_plans"
    ).fetchall()

    member_rows = conn.execute(
        """
        SELECT
            m.member_id,
            m.subscriber_id,
            m.relationship_code,
            m.family_id,
            c.client_code,
            m.client_id
        FROM members m
        JOIN clients c
          ON m.client_id = c.client_id
        """
    ).fetchall()

    eligibility_rows = conn.execute(
        """
        SELECT
            ep.member_id,
            p.plan_code,
            ep.coverage_start,
            ep.coverage_end
        FROM eligibility_periods ep
        JOIN benefit_plans p
          ON ep.plan_id = p.plan_id
        """
    ).fetchall()

    member_eligibility: dict[str, list[dict[str, Any]]] = {}
    for row in eligibility_rows:
        member_eligibility.setdefault(row["member_id"], []).append(
            {
                "plan_code": row["plan_code"],
                "coverage_start": row["coverage_start"],
                "coverage_end": row["coverage_end"],
            }
        )

    return {
        "client_codes": {row["client_code"] for row in client_rows},
        "vendor_codes": {row["vendor_code"] for row in vendor_rows},
        "plan_codes": {row["plan_code"] for row in plan_rows},
        "client_id_by_code": {row["client_code"]: row["client_id"] for row in client_rows},
        "vendor_id_by_code": {row["vendor_code"]: row["vendor_id"] for row in vendor_rows},
        "plan_by_code": {
            row["plan_code"]: {
                "plan_id": row["plan_id"],
                "benefit_year": row["benefit_year"],
                "preventive_exempt_flag": row["preventive_exempt_flag"],
            }
            for row in plan_rows
        },
        "member_map": {
            row["member_id"]: {
                "subscriber_id": row["subscriber_id"],
                "relationship_code": row["relationship_code"],
                "family_id": row["family_id"],
                "client_code": row["client_code"],
                "client_id": row["client_id"],
            }
            for row in member_rows
        },
        "member_eligibility": member_eligibility,
    }


def get_validated_claim_files(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT *
        FROM inbound_files
        WHERE file_type = 'CLAIMS'
          AND processing_status = 'VALIDATED'
        ORDER BY file_id
        """,
        db_path=db_path
    )


def _read_claims_csv(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Claims file not found: {file_path}")

    if not file_path.is_file():
        raise FileNotFoundError(f"Claims path is not a file: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except EmptyDataError as exc:
        raise ValueError(f"Claims file is empty: {file_path}") from exc
    except ParserError as exc:
        raise ValueError(f"Claims file could not be parsed as CSV: {file_path}") from exc

    if df.empty:
        raise ValueError(f"Claims file contains no data rows: {file_path}")

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


def _find_existing_claim_record_id(conn, row: pd.Series) -> int | None:
    claim_id = row.get("claim_id")
    line_id = row.get("line_id")

    existing = conn.execute(
        "SELECT claim_record_id FROM claims WHERE claim_id = ? AND line_id = ?",
        (claim_id, line_id),
    ).fetchone()

    return existing["claim_record_id"] if existing else None


def _build_existing_claim_duplicate_issue(
    claim_id: Any,
    line_id: Any,
) -> dict[str, str]:
    return {
        "issue_type": "CLAIMS",
        "issue_subtype": "DUPLICATE_CLAIM_EXISTING_RECORD",
        "severity": "HIGH",
        "issue_description": (
            f"Claim already exists in database for claim_id={claim_id}, line_id={line_id}"
        ),
    }


def insert_claim_row(
    conn,
    row: pd.Series,
    file_id: int,
    row_number: int,
    ref_data: dict[str, Any],
) -> int:
    client_id = ref_data["client_id_by_code"][row["client_code"]]
    vendor_id = ref_data["vendor_id_by_code"].get(row["vendor_code"])
    plan_info = ref_data["plan_by_code"][row["plan_code"]]
    plan_id = plan_info["plan_id"]

    cursor = conn.execute(
        """
        INSERT INTO claims (
            claim_id,
            line_id,
            member_id,
            subscriber_id,
            client_id,
            plan_id,
            vendor_id,
            service_date,
            paid_date,
            allowed_amount,
            paid_amount,
            member_responsibility,
            deductible_amount,
            coinsurance_amount,
            copay_amount,
            preventive_flag,
            reversal_flag,
            claim_status,
            source_file_id,
            source_row_number
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["claim_id"],
            row["line_id"],
            row["member_id"],
            row["subscriber_id"],
            client_id,
            plan_id,
            vendor_id,
            row["service_date"],
            row["paid_date"] if pd.notna(row.get("paid_date")) else None,
            float(row["allowed_amount"]),
            float(row["paid_amount"]),
            float(row["member_responsibility"]),
            float(row["deductible_amount"]),
            float(row["coinsurance_amount"]),
            float(row["copay_amount"]),
            1 if _is_truthy_flag(row.get("preventive_flag")) else 0,
            1 if _normalize_string(row.get("claim_status")).upper() == "REVERSED" else 0,
            row["claim_status"],
            file_id,
            row_number,
        ),
    )
    return cursor.lastrowid


def insert_accumulator_transactions(conn, txn_rows: list[dict[str, Any]]) -> None:
    for txn in txn_rows:
        conn.execute(
            """
            INSERT INTO accumulator_transactions (
                member_id,
                family_id,
                client_id,
                plan_id,
                claim_record_id,
                benefit_year,
                accumulator_type,
                delta_amount,
                service_date,
                source_type,
                source_file_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                txn["member_id"],
                txn["family_id"],
                txn["client_id"],
                txn["plan_id"],
                txn["claim_record_id"],
                txn["benefit_year"],
                txn["accumulator_type"],
                txn["delta_amount"],
                txn["service_date"],
                "CLAIM",
                txn["source_file_id"],
            ),
        )


def process_claim_files(db_path: str | Path | None = None) -> None:
    files = get_validated_claim_files(db_path=db_path)

    if not files:
        logger.info("No VALIDATED claims files available for processing.")
        return

    logger.info("Starting claims processing for %s file(s).", len(files))

    with db_session(db_path) as conn:
        ref_data = load_reference_data(conn)

        for file_record in files:
            file_id = file_record["file_id"]
            file_path = Path(file_record["landing_path"])

            run_id = _start_processing_run(conn, "CLAIMS_LOAD", file_id)

            rows_read = 0
            rows_passed = 0
            rows_failed = 0
            total_issue_count = 0

            try:
                df = _read_claims_csv(file_path)
                rows_read = len(df)

                claim_rows = [row for _, row in df.iterrows()]
                batch_issues_by_row: dict[int, list[dict[str, str]]] = {}

                duplicate_issue_results = validate_duplicate_claim_rows(claim_rows)
                for result in duplicate_issue_results:
                    batch_issues_by_row.setdefault(result["row_number"], []).append(result["issue"])

                logger.info(
                    "Claims batch validation complete: file_id=%s rows_read=%s batch_issue_rows=%s",
                    file_id,
                    rows_read,
                    len(batch_issues_by_row),
                )

                for row_idx, row in df.iterrows():
                    row_number = row_idx + 2

                    schema_issues = validate_claim_row_schema(row)
                    amount_issues = validate_claim_amount_relationships(row)
                    referential_issues = validate_claim_row_referential_and_eligibility(row, ref_data)
                    batch_issues = batch_issues_by_row.get(row_number, [])
                    all_issues = schema_issues + amount_issues + referential_issues + batch_issues

                    client_id, vendor_id = _resolve_client_vendor_ids(row, ref_data)

                    existing_claim_record_id = None
                    if not all_issues:
                        existing_claim_record_id = _find_existing_claim_record_id(conn, row)
                        if existing_claim_record_id is not None:
                            all_issues.append(
                                _build_existing_claim_duplicate_issue(
                                    claim_id=row.get("claim_id"),
                                    line_id=row.get("line_id"),
                                )
                            )

                    if all_issues:
                        issue_ids = create_claim_row_issues(
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
                        rows_failed += 1
                        total_issue_count += len(issue_ids)
                        continue

                    claim_record_id = insert_claim_row(
                        conn=conn,
                        row=row,
                        file_id=file_id,
                        row_number=row_number,
                        ref_data=ref_data,
                    )

                    plan_info = ref_data["plan_by_code"][row["plan_code"]]
                    member_info = ref_data["member_map"][row["member_id"]]

                    claim_row = {
                        "claim_record_id": claim_record_id,
                        "member_id": row["member_id"],
                        "client_id": ref_data["client_id_by_code"][row["client_code"]],
                        "plan_id": plan_info["plan_id"],
                        "service_date": row["service_date"],
                        "source_file_id": file_id,
                        "deductible_amount": float(row["deductible_amount"]),
                        "coinsurance_amount": float(row["coinsurance_amount"]),
                        "copay_amount": float(row["copay_amount"]),
                        "preventive_flag": 1 if _is_truthy_flag(row.get("preventive_flag")) else 0,
                    }

                    txn_rows = derive_accumulator_transactions(
                        claim_row=claim_row,
                        plan_row=plan_info,
                        member_row=member_info,
                    )
                    insert_accumulator_transactions(conn, txn_rows)

                    rows_passed += 1

                rebuild_accumulator_snapshots(conn, source_file_id=file_id)
                anomaly_count = detect_accumulator_anomalies(conn)
                total_issue_count += anomaly_count

                create_support_cases_from_open_issues(conn)
                evaluate_open_slas(conn)

                _finalize_processing_run_success(conn, run_id, file_id, "CLAIMS_LOAD", rows_read, rows_passed, rows_failed, total_issue_count)
                _update_inbound_file_status(conn, file_id, "PROCESSED", total_issue_count)
                _insert_audit_log(
                    conn,
                    "CLAIMS_LOADED",
                    "inbound_files",
                    str(file_id),
                    run_id,
                    file_id,
                    f"Claims processing complete: rows_read={rows_read}, rows_passed={rows_passed}, rows_failed={rows_failed}, total_issues={total_issue_count}",
                )

            except (FileNotFoundError, ValueError, KeyError) as exc:
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

    logger.info("Processed %s claims file(s).", len(files))


if __name__ == "__main__":
    process_claim_files()