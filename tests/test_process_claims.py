from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.common.db import fetch_one, fetch_all, get_connection
from src.processing.process_claims import process_claim_files


def write_csv(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_process_claim_file_success(temp_db_path, tmp_path):
    conn = get_connection(temp_db_path)

    file_path = tmp_path / "claims_valid.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C001",
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
        ],
    )

    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (10, "claims_valid.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (10,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_read"] == 1
    assert run["rows_passed"] == 1
    assert run["rows_failed"] == 0
    assert run["run_status"] in {"SUCCESS", "PARTIAL_SUCCESS"}

    claim = fetch_one(
        "SELECT claim_id, line_id, source_file_id FROM claims WHERE source_file_id = ?",
        (10,),
        db_path=temp_db_path,
    )
    assert claim is not None
    assert claim["claim_id"] == "C001"
    assert claim["line_id"] == "1"

    file_record = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (10,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] == "PROCESSED"


def test_process_claim_file_duplicate_in_batch_creates_issue(temp_db_path, tmp_path):
    conn = get_connection(temp_db_path)

    row = {
        "claim_id": "C100",
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

    file_path = tmp_path / "claims_duplicate_batch.csv"
    write_csv(file_path, [row, row])

    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (11, "claims_duplicate_batch.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ? AND issue_subtype = 'DUPLICATE_CLAIM_EXACT'
        """,
        (11,),
        db_path=temp_db_path,
    )
    assert issue is not None

    run = fetch_one(
        """
        SELECT rows_read, rows_passed, rows_failed
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (11,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_read"] == 2
    assert run["rows_failed"] >= 1


def test_process_claim_file_duplicate_existing_record_creates_issue(temp_db_path, tmp_path):
    conn = get_connection(temp_db_path)
    try:
        client_id = conn.execute(
            "SELECT client_id FROM clients WHERE client_code = ?",
            ("CASCADE",),
        ).fetchone()[0]
        plan_id = conn.execute(
            "SELECT plan_id FROM benefit_plans WHERE plan_code = ?",
            ("PLN-001",),
        ).fetchone()[0]
        vendor_id = conn.execute(
            "SELECT vendor_id FROM vendors WHERE vendor_code = ?",
            ("MEDIPROC",),
        ).fetchone()[0]

        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (500, "seed_existing_claim.csv", "CLAIMS", "seed://existing", "PROCESSED", 0),
        )

        conn.execute(
            """
            INSERT INTO claims (
                claim_id, line_id, member_id, subscriber_id, client_id, plan_id, vendor_id,
                service_date, paid_date, allowed_amount, paid_amount, member_responsibility,
                deductible_amount, coinsurance_amount, copay_amount, preventive_flag,
                reversal_flag, claim_status, source_file_id, source_row_number
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "C200", "1", "MBR-001", "SUB-001", client_id, plan_id, vendor_id,
                "2025-02-01", "2025-02-10", 100.0, 80.0, 20.0,
                10.0, 5.0, 5.0, 0, 0, "PAID", 500, 1
            ),
        )

        file_path = tmp_path / "claims_existing_duplicate.csv"
        write_csv(
            file_path,
            [
                {
                    "claim_id": "C200",
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
            ],
        )

        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (12, "claims_existing_duplicate.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_claim_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ? AND issue_subtype = 'DUPLICATE_CLAIM_EXISTING_RECORD'
        """,
        (12,),
        db_path=temp_db_path,
    )
    assert issue is not None

    run = fetch_one(
        """
        SELECT rows_passed, rows_failed
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (12,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_passed"] == 0
    assert run["rows_failed"] == 1


def test_process_claim_empty_file_marks_failed(temp_db_path, tmp_path):
    file_path = tmp_path / "claims_empty.csv"
    file_path.write_text("")

    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (30, "claims_empty.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError):
        process_claim_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, notes
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (30,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] == "FAILED"
    assert "empty" in run["notes"].lower()

    file_record = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (30,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] == "FAILED"


def test_process_claim_malformed_csv_marks_failed(temp_db_path, tmp_path):
    file_path = tmp_path / "claims_bad.csv"
    file_path.write_text('"claim_id","line_id"\n"C1","1"\n"bad')

    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (31, "claims_bad.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError):
        process_claim_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, notes
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (31,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] == "FAILED"
    assert "parse" in run["notes"].lower() or "csv" in run["notes"].lower()

    file_record = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (31,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] == "FAILED"


def test_process_claim_invalid_amount_relationship_creates_issue(temp_db_path, tmp_path):
    file_path = tmp_path / "claims_invalid_amount.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C300",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-02-01",
                "paid_date": "2025-02-10",
                "allowed_amount": 100.00,
                "paid_amount": 120.00,
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
            (32, "claims_invalid_amount.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_claim_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (32,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_read"] == 1
    assert run["rows_passed"] == 0
    assert run["rows_failed"] == 1
    assert run["issue_count"] >= 1

    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'PAID_EXCEEDS_ALLOWED'
        """,
        (32,),
        db_path=temp_db_path,
    )
    assert issue is not None


def test_process_claim_ineligible_claim_creates_issue(temp_db_path, tmp_path):
    file_path = tmp_path / "claims_ineligible.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C301",
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
            (33, "claims_ineligible.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_claim_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (33,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_read"] == 1
    assert run["rows_passed"] == 0
    assert run["rows_failed"] == 1
    assert run["issue_count"] >= 1

    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ?
          AND issue_subtype = 'INELIGIBLE_CLAIM'
        """,
        (33,),
        db_path=temp_db_path,
    )
    assert issue is not None


def test_process_claim_mixed_valid_and_invalid_rows_partial_success(temp_db_path, tmp_path):
    conn = get_connection(temp_db_path)

    file_path = tmp_path / "claims_mixed.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C004",
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
            },
            {
                "claim_id": "C005",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "INVALID_MEMBER",
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
            },
        ],
    )

    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (18, "claims_mixed.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, rows_read, rows_passed, rows_failed, issue_count
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (18,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["rows_read"] == 2
    assert run["rows_passed"] == 1
    assert run["rows_failed"] == 1
    assert run["issue_count"] >= 1
    assert run["run_status"] == "PARTIAL_SUCCESS"


def test_process_claim_creates_accumulator_transactions(temp_db_path, tmp_path):
    file_path = tmp_path / "claims_acc_txn.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C302",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-03-01",
                "paid_date": "2025-03-10",
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
            (34, "claims_acc_txn.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_claim_files(db_path=temp_db_path)

    claim = fetch_one(
        """
        SELECT claim_record_id
        FROM claims
        WHERE source_file_id = ?
        """,
        (34,),
        db_path=temp_db_path,
    )
    assert claim is not None

    txn_rows = fetch_all(
        """
        SELECT accumulator_type, delta_amount, source_file_id, claim_record_id
        FROM accumulator_transactions
        WHERE source_file_id = ?
        ORDER BY accumulator_txn_id
        """,
        (34,),
        db_path=temp_db_path,
    )
    assert len(txn_rows) >= 1
    assert all(row["source_file_id"] == 34 for row in txn_rows)
    assert all(row["claim_record_id"] == claim["claim_record_id"] for row in txn_rows)


def test_process_claim_writes_audit_log(temp_db_path, tmp_path):
    file_path = tmp_path / "claims_audit.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C303",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-04-01",
                "paid_date": "2025-04-10",
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
            (35, "claims_audit.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    process_claim_files(db_path=temp_db_path)

    audit = fetch_one(
        """
        SELECT event_type, entity_name, entity_key, file_id
        FROM audit_log
        WHERE file_id = ?
          AND event_type = 'CLAIMS_LOADED'
        ORDER BY audit_id DESC
        LIMIT 1
        """,
        (35,),
        db_path=temp_db_path,
    )
    assert audit is not None
    assert audit["event_type"] == "CLAIMS_LOADED"
    assert audit["entity_name"] == "inbound_files"
    assert audit["entity_key"] == "35"
    assert audit["file_id"] == 35


def test_process_claim_missing_file_marks_failed(temp_db_path):
    conn = get_connection(temp_db_path)
    try:
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (13, "missing_claims.csv", "CLAIMS", r"C:\does_not_exist\missing_claims.csv", "VALIDATED", 0),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(FileNotFoundError):
        process_claim_files(db_path=temp_db_path)

    run = fetch_one(
        """
        SELECT run_status, notes
        FROM processing_runs
        WHERE file_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (13,),
        db_path=temp_db_path,
    )
    assert run is not None
    assert run["run_status"] == "FAILED"
    assert "not found" in run["notes"].lower()

    file_record = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (13,),
        db_path=temp_db_path,
    )
    assert file_record is not None
    assert file_record["processing_status"] == "FAILED"