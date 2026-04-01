from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.db import fetch_all, fetch_one, get_connection
from src.processing.process_eligibility import process_eligibility_files


def write_csv(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_eligibility_issue_creates_support_case(temp_db_path, tmp_path):
    """Test that eligibility issue creation flows into support case generation."""
    file_path = tmp_path / "eligibility_issue.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "BADVENDOR",  # Invalid vendor
                "subscriber_id": "SUB-100",
                "member_id": "MBR-100",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G100",
            }
        ],
    )

    conn = get_connection(temp_db_path)
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (50, "eligibility_issue.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    # Check issue created - target HIGH severity issue that creates cases
    issue = fetch_one(
        """
        SELECT issue_id, issue_subtype, severity
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'UNKNOWN_MEMBER'
        """,
        (50,),
        db_path=temp_db_path,
    )
    assert issue is not None
    assert issue["severity"] == "HIGH"

    # Check support case created
    support_case = fetch_one(
        """
        SELECT case_id, status
        FROM support_cases
        WHERE issue_id = ?
        """,
        (issue["issue_id"],),
        db_path=temp_db_path,
    )
    assert support_case is not None
    assert support_case["status"] == "OPEN"


def test_eligibility_issue_generates_sla_tracking(temp_db_path, tmp_path):
    """Test that eligibility support case generates SLA tracking."""
    file_path = tmp_path / "eligibility_sla.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "BADVENDOR",
                "subscriber_id": "SUB-101",
                "member_id": "MBR-101",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G101",
            }
        ],
    )

    conn = get_connection(temp_db_path)
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (51, "eligibility_sla.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_id, issue_subtype, severity
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'UNKNOWN_MEMBER'
        """,
        (51,),
        db_path=temp_db_path,
    )
    assert issue is not None
    assert issue["severity"] == "HIGH"

    support_case = fetch_one(
        """
        SELECT case_id
        FROM support_cases
        WHERE issue_id = ?
        """,
        (issue["issue_id"],),
        db_path=temp_db_path,
    )
    assert support_case is not None

    # Check SLA tracking
    sla = fetch_one(
        """
        SELECT sla_id, case_id, status
        FROM sla_tracking
        WHERE case_id = ?
        """,
        (support_case["case_id"],),
        db_path=temp_db_path,
    )
    assert sla is not None
    assert sla["status"] == "OPEN"


def test_mixed_eligibility_file_creates_only_expected_issue_count(temp_db_path, tmp_path):
    """Test that multiple issues in one file generate expected number of cases."""
    file_path = tmp_path / "eligibility_mixed_issues.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "BADVENDOR1",
                "subscriber_id": "SUB-102",
                "member_id": "MBR-102",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G102",
            },
            {
                "client_code": "CASCADE",
                "vendor_code": "BADVENDOR2",
                "subscriber_id": "SUB-103",
                "member_id": "MBR-103",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G103",
            },
        ],
    )

    conn = get_connection(temp_db_path)
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (52, "eligibility_mixed_issues.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    # Check issues created (2 invalid vendors, 2 unknown members)
    issues = fetch_all(
        """
        SELECT issue_id, issue_subtype
        FROM data_quality_issues
        WHERE file_id = ?
        ORDER BY issue_id
        """,
        (52,),
        db_path=temp_db_path,
    )
    assert len(issues) == 4
    subtypes = [row["issue_subtype"] for row in issues]
    assert subtypes.count("UNKNOWN_VENDOR") == 2
    assert subtypes.count("UNKNOWN_MEMBER") == 2

    # Check support cases - only HIGH severity issues create cases
    support_cases = fetch_all(
        """
        SELECT case_id
        FROM support_cases
        WHERE issue_id IN ({})
        """.format(','.join('?' for _ in issues)),
        tuple(issue["issue_id"] for issue in issues),
        db_path=temp_db_path,
    )
    assert len(support_cases) == 2  # Only UNKNOWN_MEMBER creates cases


def test_processed_eligibility_file_writes_audit_and_run_records_consistently(temp_db_path, tmp_path):
    """Test that processed eligibility file writes audit and run records."""
    file_path = tmp_path / "eligibility_audit_run.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-104",
                "member_id": "MBR-104",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G104",
            }
        ],
    )

    conn = get_connection(temp_db_path)
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (53, "eligibility_audit_run.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    # Check processing run
    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (53,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] in {"SUCCESS", "PARTIAL_SUCCESS"}
    assert run["rows_read"] == 1

    # Check audit log
    audit = fetch_one(
        """
        SELECT event_type, entity_name, entity_key
        FROM audit_log
        WHERE file_id = ? AND event_type = 'ELIGIBILITY_LOADED'
        ORDER BY audit_id DESC
        LIMIT 1
        """,
        (53,),
        db_path=temp_db_path,
    )
    assert audit is not None
    assert audit["entity_name"] == "inbound_files"
    assert audit["entity_key"] == "53"


def test_high_severity_eligibility_overlap_creates_support_case(temp_db_path, tmp_path):
    """Test that high-severity eligibility overlap creates support case."""
    file_path = tmp_path / "eligibility_overlap_case.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-105",
                "member_id": "MBR-105",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-06-30",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G105",
            },
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-105",
                "member_id": "MBR-105",
                "plan_code": "PLN-001",
                "coverage_start": "2025-06-15",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G105",
            },
        ],
    )

    conn = get_connection(temp_db_path)
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (54, "eligibility_overlap_case.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    # Check issue created
    issue = fetch_one(
        """
        SELECT issue_id, severity
        FROM data_quality_issues
        WHERE file_id = ? AND issue_subtype = 'ELIGIBILITY_OVERLAP'
        """,
        (54,),
        db_path=temp_db_path,
    )
    assert issue is not None
    assert issue["severity"] == "HIGH"

    # Check support case created
    support_case = fetch_one(
        """
        SELECT case_id, status
        FROM support_cases
        WHERE issue_id = ?
        """,
        (issue["issue_id"],),
        db_path=temp_db_path,
    )
    assert support_case is not None
    assert support_case["status"] == "OPEN"


def test_eligibility_support_case_gets_sla_row(temp_db_path, tmp_path):
    """Test that eligibility support case gets SLA row."""
    file_path = tmp_path / "eligibility_sla_case.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-106",
                "member_id": "MBR-106",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-06-30",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G106",
            },
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-106",
                "member_id": "MBR-106",
                "plan_code": "PLN-001",
                "coverage_start": "2025-06-15",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G106",
            },
        ],
    )

    conn = get_connection(temp_db_path)
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (55, "eligibility_sla_case.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_id, severity
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'ELIGIBILITY_OVERLAP'
        """,
        (55,),
        db_path=temp_db_path,
    )
    assert issue is not None
    assert issue["severity"] == "HIGH"

    support_case = fetch_one(
        """
        SELECT case_id
        FROM support_cases
        WHERE issue_id = ?
        """,
        (issue["issue_id"],),
        db_path=temp_db_path,
    )
    assert support_case is not None

    # Check SLA created
    sla = fetch_one(
        """
        SELECT sla_id, status
        FROM sla_tracking
        WHERE case_id = ?
        """,
        (support_case["case_id"],),
        db_path=temp_db_path,
    )
    assert sla is not None
    assert sla["status"] == "OPEN"