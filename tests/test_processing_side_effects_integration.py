from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.db import fetch_one, fetch_all, get_connection
from src.processing.process_claims import process_claim_files
from src.processing.process_eligibility import process_eligibility_files


def write_csv(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_high_severity_claim_issue_creates_support_case_and_sla(temp_db_path, tmp_path):
    file_path = tmp_path / "claims_ineligible_case.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C400",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2026-02-01",
                "paid_date": "2026-02-10",
                "allowed_amount": 100.00,
                "paid_amount": 80.00,
                "member_responsibility": 20.00,
                "deductible_amount": 10.00,
                "coinsurance_amount": 5.00,
                "copay_amount": 5.00,
                "claim_status": "PAID",
                "preventive_flag": "0",
            }
        ],
    )

    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (40, "claims_ineligible_case.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_claim_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_id, severity, issue_subtype, file_id
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'INELIGIBLE_CLAIM'
        ORDER BY issue_id DESC
        LIMIT 1
        """,
        (40,),
        db_path=temp_db_path,
    )
    assert issue is not None
    assert issue["severity"] == "HIGH"

    case_row = fetch_one(
        """
        SELECT case_id, issue_id, status, severity, file_id
        FROM support_cases
        WHERE issue_id = ?
        ORDER BY case_id DESC
        LIMIT 1
        """,
        (issue["issue_id"],),
        db_path=temp_db_path,
    )
    assert case_row is not None
    assert case_row["issue_id"] == issue["issue_id"]
    assert case_row["status"] == "OPEN"
    assert case_row["severity"] == "HIGH"
    assert case_row["file_id"] == 40

    sla = fetch_one(
        """
        SELECT case_id, status, target_hours
        FROM sla_tracking
        WHERE case_id = ?
        ORDER BY sla_id DESC
        LIMIT 1
        """,
        (case_row["case_id"],),
        db_path=temp_db_path,
    )
    assert sla is not None
    assert sla["case_id"] == case_row["case_id"]
    assert sla["status"] in {"OPEN", "AT_RISK", "BREACHED", "CLOSED"}
    assert sla["target_hours"] == 24 or sla["target_hours"] == 8


def test_low_severity_duplicate_exact_claim_does_not_create_support_case(temp_db_path, tmp_path):
    row = {
        "claim_id": "C401",
        "line_id": "1",
        "client_code": "CASCADE",
        "vendor_code": "MEDIPROC",
        "member_id": "MBR-001",
        "subscriber_id": "SUB-001",
        "plan_code": "PLN-001",
        "service_date": "2025-02-01",
        "paid_date": "2025-02-10",
        "allowed_amount": 100.00,
        "paid_amount": 80.00,
        "member_responsibility": 20.00,
        "deductible_amount": 10.00,
        "coinsurance_amount": 5.00,
        "copay_amount": 5.00,
        "claim_status": "PAID",
        "preventive_flag": "0",
    }

    file_path = tmp_path / "claims_dup_exact_case.csv"
    write_csv(file_path, [row, row])

    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (41, "claims_dup_exact_case.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_claim_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_id, severity
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'DUPLICATE_CLAIM_EXACT'
        ORDER BY issue_id DESC
        LIMIT 1
        """,
        (41,),
        db_path=temp_db_path,
    )
    assert issue is not None
    assert issue["severity"] == "LOW"

    case_row = fetch_one(
        """
        SELECT case_id
        FROM support_cases
        WHERE issue_id = ?
        """,
        (issue["issue_id"],),
        db_path=temp_db_path,
    )
    assert case_row is None


def test_high_severity_eligibility_overlap_creates_support_case_and_sla(temp_db_path, tmp_path):
    file_path = tmp_path / "eligibility_overlap_case.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-099",
                "member_id": "MBR-099",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-06-30",
                "status": "ACTIVE",
                "relationship_code": "SUB",
            },
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-099",
                "member_id": "MBR-099",
                "plan_code": "PLN-001",
                "coverage_start": "2025-06-15",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
            },
        ],
    )

    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (42, "eligibility_overlap_case.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_eligibility_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_id, severity, issue_subtype
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'ELIGIBILITY_OVERLAP'
        ORDER BY issue_id DESC
        LIMIT 1
        """,
        (42,),
        db_path=temp_db_path,
    )
    assert issue is not None
    assert issue["severity"] == "HIGH"

    case_row = fetch_one(
        """
        SELECT case_id, issue_id, status
        FROM support_cases
        WHERE issue_id = ?
        ORDER BY case_id DESC
        LIMIT 1
        """,
        (issue["issue_id"],),
        db_path=temp_db_path,
    )
    # This assertion depends on whether eligibility processing currently calls support case generation.
    # If not, this will reveal a behavior gap rather than a flaky test.
    assert case_row is not None
    assert case_row["issue_id"] == issue["issue_id"]
    assert case_row["status"] == "OPEN"

    sla = fetch_one(
        """
        SELECT case_id
        FROM sla_tracking
        WHERE case_id = ?
        ORDER BY sla_id DESC
        LIMIT 1
        """,
        (case_row["case_id"],),
        db_path=temp_db_path,
    )
    assert sla is not None