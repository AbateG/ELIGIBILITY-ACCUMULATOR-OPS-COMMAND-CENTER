from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.db import fetch_all, fetch_one, get_connection
from src.processing.process_claims import process_claim_files


def write_csv(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_claim_issue_creates_support_case(temp_db_path, tmp_path):
    """Test that processing an invalid claim creates a support case."""
    file_path = tmp_path / "claims_invalid.csv"
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
                "service_date": "2025-02-01",
                "paid_date": "2025-02-10",
                "allowed_amount": 100.00,
                "paid_amount": 120.00,  # Invalid: paid > allowed
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
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (40, "claims_invalid.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    # Check issue created
    issue = fetch_one(
        """
        SELECT issue_id, issue_subtype
        FROM data_quality_issues
        WHERE file_id = ? AND issue_subtype = 'PAID_EXCEEDS_ALLOWED'
        """,
        (40,),
        db_path=temp_db_path,
    )
    assert issue is not None

    # Check support case created from issue
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


def test_claim_issue_generates_sla_tracking(temp_db_path, tmp_path):
    """Test that support case creation generates SLA tracking."""
    file_path = tmp_path / "claims_sla.csv"
    write_csv(
        file_path,
        [
            {
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
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (41, "claims_sla.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_id
        FROM data_quality_issues
        WHERE file_id = ?
        """,
        (41,),
        db_path=temp_db_path,
    )
    assert issue is not None

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

    # Check SLA tracking created
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


def test_successful_claim_processing_updates_snapshot(temp_db_path, tmp_path):
    """Test that successful claim processing updates accumulator snapshot."""
    file_path = tmp_path / "claims_snapshot.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C402",
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
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (42, "claims_snapshot.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    # Check claim inserted
    claim = fetch_one(
        "SELECT claim_record_id FROM claims WHERE source_file_id = ?",
        (42,),
        db_path=temp_db_path,
    )
    assert claim is not None

    # Check accumulator transactions inserted
    txns = fetch_all(
        "SELECT * FROM accumulator_transactions WHERE source_file_id = ?",
        (42,),
        db_path=temp_db_path,
    )
    assert len(txns) > 0

    # Check snapshot updated
    snapshot = fetch_one(
        """
        SELECT member_id, individual_deductible_accum, individual_oop_accum
        FROM accumulator_snapshots
        WHERE member_id = 'MBR-001'
        """,
        db_path=temp_db_path,
    )
    assert snapshot is not None
    assert snapshot["individual_deductible_accum"] >= 10.0
    assert snapshot["individual_oop_accum"] >= 10.0


def test_duplicate_existing_claim_does_not_create_accumulator_transactions(temp_db_path, tmp_path):
    """Test that processing a duplicate existing claim does not create new accumulator transactions."""
    # First, seed an existing claim
    conn = get_connection(temp_db_path)
    client_id = conn.execute("SELECT client_id FROM clients WHERE client_code = 'CASCADE'").fetchone()[0]
    plan_id = conn.execute("SELECT plan_id FROM benefit_plans WHERE plan_code = 'PLN-001'").fetchone()[0]
    vendor_id = conn.execute("SELECT vendor_id FROM vendors WHERE vendor_code = 'MEDIPROC'").fetchone()[0]

    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (500, "seed_claim.csv", "CLAIMS", "seed://", "PROCESSED", 0),
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
            "C403", "1", "MBR-001", "SUB-001", client_id, plan_id, vendor_id,
            "2025-04-01", "2025-04-10", 100.0, 80.0, 20.0,
            10.0, 5.0, 5.0, 0, 0, "PAID", 500, 1
        ),
    )
    conn.commit()

    # Now process duplicate
    file_path = tmp_path / "claims_duplicate.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C403",
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

    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (43, "claims_duplicate.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    # Check issue created for duplicate
    issue = fetch_one(
        """
        SELECT issue_subtype
        FROM data_quality_issues
        WHERE file_id = ? AND issue_subtype = 'DUPLICATE_CLAIM_EXISTING_RECORD'
        """,
        (43,),
        db_path=temp_db_path,
    )
    assert issue is not None

    # Check no new transactions for the duplicate file
    txns = fetch_all(
        "SELECT * FROM accumulator_transactions WHERE source_file_id = ?",
        (43,),
        db_path=temp_db_path,
    )
    assert len(txns) == 0


def test_multiple_claims_accumulate_snapshot_totals(temp_db_path, tmp_path):
    """Test that processing multiple claims accumulates snapshot totals correctly."""
    file_path = tmp_path / "claims_multiple.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C404",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-05-01",
                "paid_date": "2025-05-10",
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
                "claim_id": "C405",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-05-15",
                "paid_date": "2025-05-20",
                "allowed_amount": 150.00,
                "paid_amount": 120.00,
                "member_responsibility": 30.00,
                "deductible_amount": 15.00,
                "coinsurance_amount": 7.50,
                "copay_amount": 7.50,
                "claim_status": "PAID",
                "preventive_flag": "0",
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
        (44, "claims_multiple.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    # Check claims inserted
    claims = fetch_all(
        "SELECT claim_record_id FROM claims WHERE source_file_id = ?",
        (44,),
        db_path=temp_db_path,
    )
    assert len(claims) == 2

    # Check snapshot reflects cumulative amounts
    snapshot = fetch_one(
        """
        SELECT individual_deductible_accum, individual_oop_accum
        FROM accumulator_snapshots
        WHERE member_id = 'MBR-001'
        """,
        db_path=temp_db_path,
    )
    assert snapshot is not None
    assert snapshot["individual_deductible_accum"] >= 25.0  # 10 + 15
    assert snapshot["individual_oop_accum"] >= 25.0  # 5+5 + 7.5+7.5


def test_high_severity_claim_issue_creates_support_case(temp_db_path, tmp_path):
    """Test that high-severity claim issue creates support case."""
    # Use PAID_EXCEEDS_ALLOWED as HIGH severity
    file_path = tmp_path / "claims_high_severity.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C406",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-06-01",
                "paid_date": "2025-06-10",
                "allowed_amount": 100.00,
                "paid_amount": 120.00,  # Exceeds allowed
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
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (45, "claims_high_severity.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    # Check issue created
    issue = fetch_one(
        """
        SELECT issue_id, severity
        FROM data_quality_issues
        WHERE file_id = ? AND issue_subtype = 'PAID_EXCEEDS_ALLOWED'
        """,
        (45,),
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


def test_created_support_case_gets_sla_row(temp_db_path, tmp_path):
    """Test that created support case gets SLA row."""
    file_path = tmp_path / "claims_sla_case.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C407",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-07-01",
                "paid_date": "2025-07-10",
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
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (46, "claims_sla_case.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    issue = fetch_one(
        """
        SELECT issue_id
        FROM data_quality_issues
        WHERE file_id = ?
        """,
        (46,),
        db_path=temp_db_path,
    )
    assert issue is not None

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


def test_successful_claim_processing_no_support_case_when_no_issues(temp_db_path, tmp_path):
    """Test that successful claim processing writes no support case when there are no open issues."""
    file_path = tmp_path / "claims_no_issues.csv"
    write_csv(
        file_path,
        [
            {
                "claim_id": "C408",
                "line_id": "1",
                "client_code": "CASCADE",
                "vendor_code": "MEDIPROC",
                "member_id": "MBR-001",
                "subscriber_id": "SUB-001",
                "plan_code": "PLN-001",
                "service_date": "2025-08-01",
                "paid_date": "2025-08-10",
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
    conn.execute(
        """
        INSERT INTO inbound_files (
            file_id, file_name, file_type, landing_path, processing_status, error_count
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (47, "claims_no_issues.csv", "CLAIMS", str(file_path), "VALIDATED", 0),
    )
    conn.commit()
    conn.close()

    process_claim_files(db_path=temp_db_path)

    # Check no issues created
    issues = fetch_all(
        "SELECT issue_id FROM data_quality_issues WHERE file_id = ?",
        (47,),
        db_path=temp_db_path,
    )
    assert len(issues) == 0

    # Check no support cases created
    cases = fetch_all(
        "SELECT case_id FROM support_cases WHERE file_id = ?",
        (47,),
        db_path=temp_db_path,
    )
    assert len(cases) == 0