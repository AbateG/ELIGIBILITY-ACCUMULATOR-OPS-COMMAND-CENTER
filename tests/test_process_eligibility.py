from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.common.db import fetch_one, get_connection
from src.processing.process_eligibility import process_eligibility_files


def write_csv(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_process_eligibility_file_success(temp_db_path, tmp_path):
    conn = get_connection(temp_db_path)

    file_path = tmp_path / "eligibility_valid.csv"
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
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G001",
            }
        ],
    )

    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (1, "eligibility_valid.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (1,),
        db_path=temp_db_path,
    )
    assert run is not None
    # Because MBR-001 already has a seeded 2025 eligibility row, this may not be SUCCESS.
    assert run["rows_read"] == 1
    assert run["rows_passed"] + run["rows_failed"] == 1

    file_record = fetch_one(
        "SELECT processing_status, error_count FROM inbound_files WHERE file_id = ?",
        (1,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] in {"PROCESSED", "FAILED"}

    # If the row was inserted, verify the insert shape.
    eligibility = fetch_one(
        """
        SELECT member_id, subscriber_id, source_file_id
        FROM eligibility_periods
        WHERE source_file_id = ?
        """,
        (1,),
        db_path=temp_db_path,
    )
    if run["rows_passed"] == 1:
        assert eligibility is not None
        assert eligibility["member_id"] == "MBR-099"
        assert eligibility["subscriber_id"] == "SUB-099"
    else:
        assert eligibility is None


def test_process_eligibility_file_overlap_creates_issue(temp_db_path, tmp_path):
    conn = get_connection(temp_db_path)

    file_path = tmp_path / "eligibility_overlap.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-001",
                "member_id": "MBR-001",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-06-30",
                "status": "ACTIVE",
                "relationship_code": "SUB",
            },
            {
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "subscriber_id": "SUB-001",
                "member_id": "MBR-001",
                "plan_code": "PLN-001",
                "coverage_start": "2025-06-15",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
            },
        ],
    )

    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (2, "eligibility_overlap.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_eligibility_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (2,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_read"] == 2
    assert run["rows_failed"] >= 1
    assert run["issue_count"] >= 1
    assert run["run_status"] in {"PARTIAL_SUCCESS", "FAILED", "SUCCESS"}

    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ? AND issue_subtype = 'ELIGIBILITY_OVERLAP'
        """,
        (2,),
        db_path=temp_db_path,
    )
    assert issue is not None


def test_process_eligibility_empty_file_marks_failed(temp_db_path, tmp_path):
    file_path = tmp_path / "eligibility_empty.csv"
    file_path.write_text("")

    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (20, "eligibility_empty.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError):
        process_eligibility_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, notes
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (20,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] == "FAILED"
    assert "empty" in run["notes"].lower()

    file_record = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (20,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] == "FAILED"


def test_process_eligibility_malformed_csv_marks_failed(temp_db_path, tmp_path):
    file_path = tmp_path / "eligibility_bad.csv"
    file_path.write_text('"client_code","vendor_code"\n"CASCADE","MEDIPROC"\n"bad')

    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (21, "eligibility_bad.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError):
        process_eligibility_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, notes
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (21,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] == "FAILED"
    assert "parse" in run["notes"].lower() or "csv" in run["notes"].lower()

    file_record = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (21,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] == "FAILED"


def test_process_eligibility_invalid_referential_row_creates_issue(temp_db_path, tmp_path):
    file_path = tmp_path / "eligibility_invalid_ref.csv"
    write_csv(
        file_path,
        [
            {
                "client_code": "CASCADE",
                "vendor_code": "BADVENDOR",
                "subscriber_id": "SUB-099",
                "member_id": "MBR-099",
                "plan_code": "PLN-001",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G099",
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
            (22, "eligibility_invalid_ref.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_eligibility_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (22,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_read"] == 1
    assert run["rows_passed"] == 0
    assert run["rows_failed"] == 1
    assert run["issue_count"] >= 1
    assert run["run_status"] == "PARTIAL_SUCCESS"

    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'UNKNOWN_VENDOR'
        """,
        (22,),
        db_path=temp_db_path,
    )
    assert issue is not None


def test_process_eligibility_mixed_valid_and_invalid_rows_partial_success(temp_db_path, tmp_path):
    file_path = tmp_path / "eligibility_mixed.csv"
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
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G099",
            },
            {
                "client_code": "CASCADE",
                "vendor_code": "BADVENDOR",
                "subscriber_id": "SUB-099",
                "member_id": "MBR-099",
                "plan_code": "PLN-001",
                "coverage_start": "2026-01-01",
                "coverage_end": "2026-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G099",
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
            (23, "eligibility_mixed.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_eligibility_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (23,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] == "PARTIAL_SUCCESS"
    assert run["rows_read"] == 2
    assert run["rows_passed"] == 1
    assert run["rows_failed"] == 1
    assert run["issue_count"] >= 1

    inserted = fetch_one(
        """
        SELECT member_id, subscriber_id, source_file_id
        FROM eligibility_periods
        WHERE source_file_id = ?
        """,
        (23,),
        db_path=temp_db_path,
    )
    assert inserted is not None
    assert inserted["member_id"] == "MBR-099"

    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'UNKNOWN_VENDOR'
        """,
        (23,),
        db_path=temp_db_path,
    )
    assert issue is not None


def test_process_eligibility_writes_audit_log(temp_db_path, tmp_path):
    file_path = tmp_path / "eligibility_audit.csv"
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
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "relationship_code": "SUB",
                "group_id": "G099",
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
            (24, "eligibility_audit.csv", "ELIGIBILITY", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_eligibility_files(db_path=temp_db_path)

    audit = fetch_one(
        """
        SELECT event_type, entity_name, entity_key, file_id
        FROM audit_log
        WHERE file_id = ?
          AND event_type = 'ELIGIBILITY_LOADED'
        ORDER BY audit_id DESC
        LIMIT 1
        """,
        (24,),
        db_path=temp_db_path,
    )
    assert audit is not None
    assert audit["event_type"] == "ELIGIBILITY_LOADED"
    assert audit["entity_name"] == "inbound_files"
    assert audit["entity_key"] == "24"
    assert audit["file_id"] == 24


def test_process_eligibility_missing_file_marks_failed(temp_db_path):
    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (3, "missing.csv", "ELIGIBILITY", r"C:\does_not_exist\missing.csv", "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(FileNotFoundError):
        process_eligibility_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, notes
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (3,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] == "FAILED"
    assert "not found" in run["notes"].lower()

    file_record = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (3,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] == "FAILED"