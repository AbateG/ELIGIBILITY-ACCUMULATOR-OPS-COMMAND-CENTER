from __future__ import annotations

import pytest

from src.accumulators.snapshot_engine import detect_accumulator_anomalies, rebuild_accumulator_snapshots
from src.common.db import fetch_all, fetch_one, get_connection


def test_rebuild_accumulator_snapshots_creates_snapshot_from_transactions(temp_db_path):
    conn = get_connection(temp_db_path)
    plan_id = None
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
            (1000, "snapshot_seed_claim.csv", "CLAIMS", "seed://snapshot", "PROCESSED", 0),
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
                "SNAP001", "1", "MBR-001", "SUB-001", client_id, plan_id, vendor_id,
                "2025-06-01", "2025-06-10", 100.0, 80.0, 20.0,
                10.0, 5.0, 5.0, 0, 0, "PAID", 1000, 1,
            ),
        )
        claim_record_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.execute(
            """
            INSERT INTO accumulator_transactions (
                member_id, family_id, client_id, plan_id, claim_record_id, benefit_year,
                accumulator_type, delta_amount, service_date, source_type, source_file_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "MBR-001", "SUB-001", client_id, plan_id, claim_record_id, 2025,
                "IND_DED", 10.0, "2025-06-01", "CLAIM", 1000,
            ),
        )
        conn.commit()

        rebuilt = rebuild_accumulator_snapshots(conn)
        assert rebuilt >= 1
        conn.commit()
    finally:
        conn.close()

    snapshot = fetch_one(
        """
        SELECT member_id, family_id, plan_id, benefit_year, individual_deductible_accum
        FROM accumulator_snapshots
        WHERE member_id = ?
          AND plan_id = ?
          AND benefit_year = ?
        """,
        ("MBR-001", plan_id, 2025),
        db_path=temp_db_path,
    )
    assert snapshot is not None
    assert snapshot["member_id"] == "MBR-001"
    assert snapshot["family_id"] == "SUB-001"
    assert snapshot["benefit_year"] == 2025
    assert snapshot["individual_deductible_accum"] == pytest.approx(10.0)


def test_detect_accumulator_anomalies_returns_zero_for_consistent_data(temp_db_path):
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

        conn.execute(
            """
            INSERT INTO accumulator_snapshots (
                member_id, family_id, client_id, plan_id, benefit_year,
                individual_deductible_accum, family_deductible_accum,
                individual_oop_accum, family_oop_accum,
                individual_deductible_met_flag, family_deductible_met_flag,
                individual_oop_met_flag, family_oop_met_flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "MBR-001", "SUB-001", client_id, plan_id, 2025,
                10.0, 10.0,
                20.0, 20.0,
                0, 0, 0, 0,
            ),
        )
        conn.commit()

        anomaly_count = detect_accumulator_anomalies(conn)
        assert anomaly_count == 0
    finally:
        conn.close()


def test_detect_accumulator_anomalies_detects_corrupt_snapshot(temp_db_path):
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

        conn.execute(
            """
            INSERT INTO accumulator_snapshots (
                member_id, family_id, client_id, plan_id, benefit_year,
                individual_deductible_accum, family_deductible_accum,
                individual_oop_accum, family_oop_accum,
                individual_deductible_met_flag, family_deductible_met_flag,
                individual_oop_met_flag, family_oop_met_flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "MBR-002", "SUB-001", client_id, plan_id, 2025,
                100.0, 0.0,
                200.0, 13000.0,  # Exceeds family_oop_max=12000.0
                0, 0, 0, 0,
            ),
        )
        conn.commit()

        anomaly_count = detect_accumulator_anomalies(conn)
        assert anomaly_count > 0
    finally:
        conn.close()


def test_reversed_claim_transactions_do_not_overstate_snapshot(temp_db_path):
    conn = get_connection(temp_db_path)
    plan_id = None
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
            (1001, "snapshot_reverse_seed.csv", "CLAIMS", "seed://reverse", "PROCESSED", 0),
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
                "SNAPREV001", "1", "MBR-003", "SUB-001", client_id, plan_id, vendor_id,
                "2025-07-01", "2025-07-10", -10.0, -10.0, -10.0,
                -10.0, 0.0, 0.0, 0, 1, "REVERSED", 1001, 1,
            ),
        )
        claim_record_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.execute(
            """
            INSERT INTO accumulator_transactions (
                member_id, family_id, client_id, plan_id, claim_record_id, benefit_year,
                accumulator_type, delta_amount, service_date, source_type, source_file_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "MBR-003", "SUB-001", client_id, plan_id, claim_record_id, 2025,
                "IND_DED", -10.0, "2025-07-01", "CLAIM", 1001,
            ),
        )
        conn.commit()

        rebuilt = rebuild_accumulator_snapshots(conn)
        assert rebuilt >= 1
        conn.commit()
    finally:
        conn.close()

    snapshot = fetch_one(
        """
        SELECT individual_deductible_accum
        FROM accumulator_snapshots
        WHERE member_id = ?
          AND plan_id = ?
          AND benefit_year = ?
        """,
        ("MBR-003", plan_id, 2025),
        db_path=temp_db_path,
    )
    assert snapshot is not None
    assert snapshot["individual_deductible_accum"] == pytest.approx(-10.0)


def test_snapshot_rebuild_requires_explicit_commit(temp_db_path):
    """Test that snapshot rebuild writes are not visible without explicit commit."""
    conn = get_connection(temp_db_path)
    plan_id = None
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

        # Insert test member
        conn.execute(
            """
            INSERT INTO members (
                member_id, subscriber_id, client_id, first_name, last_name, dob,
                relationship_code, family_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("MBR-COMMIT", "SUB-COMMIT", client_id, "Test", "Member", "1980-01-01", "SELF", "SUB-COMMIT"),
        )

        # Insert test data
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (2000, "snapshot_commit_test.csv", "CLAIMS", "seed://commit_test", "PROCESSED", 0),
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
                "COMMIT-TEST", "1", "MBR-COMMIT", "SUB-COMMIT", client_id, plan_id, vendor_id,
                "2025-08-01", "2025-08-10", 50.0, 40.0, 10.0,
                5.0, 2.5, 2.5, 0, 0, "PAID", 2000, 1,
            ),
        )
        claim_record_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.execute(
            """
            INSERT INTO accumulator_transactions (
                member_id, family_id, client_id, plan_id, claim_record_id, benefit_year,
                accumulator_type, delta_amount, service_date, source_type, source_file_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "MBR-COMMIT", "SUB-COMMIT", client_id, plan_id, claim_record_id, 2025,
                "IND_DED", 5.0, "2025-08-01", "CLAIM", 2000,
            ),
        )
        conn.commit()

        # Rebuild without committing
        rebuilt = rebuild_accumulator_snapshots(conn)
        assert rebuilt >= 1

        # Check snapshot exists on same connection
        snapshot_same_conn = conn.execute(
            """
            SELECT member_id, individual_deductible_accum
            FROM accumulator_snapshots
            WHERE member_id = ? AND plan_id = ? AND benefit_year = ?
            """,
            ("MBR-COMMIT", plan_id, 2025),
        ).fetchone()
        assert snapshot_same_conn is not None
        assert snapshot_same_conn["individual_deductible_accum"] == pytest.approx(5.0)

        # Close without committing
    finally:
        conn.close()

    # Verify not visible on new connection
    snapshot_new_conn = fetch_one(
        """
        SELECT member_id, individual_deductible_accum
        FROM accumulator_snapshots
        WHERE member_id = ? AND plan_id = ? AND benefit_year = ?
        """,
        ("MBR-COMMIT", plan_id, 2025),
        db_path=temp_db_path,
    )
    assert snapshot_new_conn is None  # Should not be visible without commit


def test_failure_finalization_durably_persists_despite_exception_re_raise(temp_db_path):
    """Test that failure finalization persists even when original exception is re-raised."""
    from src.processing.processing_helpers import finalize_file_failure_durably
    from src.common.db import get_connection

    conn = get_connection(temp_db_path)
    try:
        client_id = conn.execute(
            "SELECT client_id FROM clients WHERE client_code = ?",
            ("CASCADE",),
        ).fetchone()[0]

        # Insert test member
        conn.execute(
            """
            INSERT INTO members (
                member_id, subscriber_id, client_id, first_name, last_name, dob,
                relationship_code, family_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("MBR-FAILURE", "SUB-FAILURE", client_id, "Test", "Failure", "1980-01-01", "SELF", "SUB-FAILURE"),
        )

        # Insert a test file
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (3000, "failure_test.csv", "CLAIMS", "seed://failure", "VALIDATED", 0),
        )

        # Start a processing run
        from src.processing.processing_helpers import _start_processing_run
        run_id = _start_processing_run(conn, "CLAIMS_LOAD", 3000)
        conn.commit()
    finally:
        conn.close()

    # Simulate failure finalization
    try:
        finalize_file_failure_durably(
            db_path=temp_db_path,
            run_id=run_id,
            file_id=3000,
            rows_read=10,
            rows_passed=5,
            rows_failed=5,
            issue_count=3,
            exc=ValueError("Test processing failure"),
        )
    except Exception:
        pass  # We expect the exception to be re-raised

    # Verify failure state was persisted despite exception
    run_status = fetch_one(
        "SELECT run_status, notes FROM processing_runs WHERE run_id = ?",
        (run_id,),
        db_path=temp_db_path,
    )
    assert run_status is not None
    assert run_status["run_status"] == "FAILED"
    assert "Test processing failure" in run_status["notes"]

    file_status = fetch_one(
        "SELECT processing_status FROM inbound_files WHERE file_id = ?",
        (3000,),
        db_path=temp_db_path,
    )
    assert file_status is not None
    assert file_status["processing_status"] == "FAILED"


def test_snapshot_upsert_updates_existing_row(temp_db_path):
    """Test that snapshot rebuild updates existing rows rather than duplicating."""
    conn = get_connection(temp_db_path)
    plan_id = None
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

        # Insert test member
        conn.execute(
            """
            INSERT INTO members (
                member_id, subscriber_id, client_id, first_name, last_name, dob,
                relationship_code, family_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("MBR-UPSERT", "SUB-UPSERT", client_id, "Test", "Upsert", "1980-01-01", "SELF", "SUB-UPSERT"),
        )

        # Insert initial snapshot
        conn.execute(
            """
            INSERT INTO accumulator_snapshots (
                member_id, family_id, client_id, plan_id, benefit_year,
                individual_deductible_accum, family_deductible_accum,
                individual_oop_accum, family_oop_accum,
                individual_deductible_met_flag, family_deductible_met_flag,
                individual_oop_met_flag, family_oop_met_flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "MBR-UPSERT", "SUB-UPSERT", client_id, plan_id, 2025,
                10.0, 10.0, 20.0, 20.0, 0, 0, 0, 0,
            ),
        )
        conn.commit()

        # Insert transaction that should update the existing snapshot
        conn.execute(
            """
            INSERT INTO inbound_files (
                file_id, file_name, file_type, landing_path, processing_status, error_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (4000, "upsert_test.csv", "CLAIMS", "seed://upsert", "PROCESSED", 0),
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
                "UPSERT-TEST", "1", "MBR-UPSERT", "SUB-UPSERT", client_id, plan_id, vendor_id,
                "2025-09-01", "2025-09-10", 50.0, 40.0, 10.0,
                15.0, 7.5, 7.5, 0, 0, "PAID", 4000, 1,
            ),
        )
        claim_record_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.execute(
            """
            INSERT INTO accumulator_transactions (
                member_id, family_id, client_id, plan_id, claim_record_id, benefit_year,
                accumulator_type, delta_amount, service_date, source_type, source_file_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "MBR-UPSERT", "SUB-UPSERT", client_id, plan_id, claim_record_id, 2025,
                "IND_DED", 15.0, "2025-09-01", "CLAIM", 4000,
            ),
        )
        conn.commit()

        # Rebuild snapshots
        rebuilt = rebuild_accumulator_snapshots(conn)
        assert rebuilt >= 1
        conn.commit()

        # Verify only one row exists and was updated
        snapshots = fetch_all(
            """
            SELECT member_id, individual_deductible_accum
            FROM accumulator_snapshots
            WHERE member_id = ? AND plan_id = ? AND benefit_year = ?
            """,
            ("MBR-UPSERT", plan_id, 2025),
            db_path=temp_db_path,
        )
        assert len(snapshots) == 1  # Should not duplicate
        assert snapshots[0]["individual_deductible_accum"] == pytest.approx(15.0)  # Rebuild calculates from transactions
    finally:
        conn.close()