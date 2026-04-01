"""
Regression tests for anomaly detection tuning.

These tests ensure that the revised anomaly detection policy is correctly implemented:
- NEGATIVE_ACCUMULATOR: MEDIUM severity (no auto-case)
- IND_OOP_EXCEEDS_MAX: CRITICAL severity (auto-case)
- FAM_OOP_EXCEEDS_MAX: CRITICAL severity (auto-case)
- FAMILY_ROLLUP_MISMATCH: MEDIUM severity (no auto-case)
"""

from __future__ import annotations

import pytest

from src.common.db import get_connection
from src.accumulators.snapshot_engine import detect_accumulator_anomalies
from src.common.db import fetch_all, fetch_one


def create_test_snapshot(conn, member_id: str, family_id: str, client_id: int, plan_id: int,
                        individual_deductible_accum: float = 0.0,
                        family_deductible_accum: float = 0.0,
                        individual_oop_accum: float = 0.0,
                        family_oop_accum: float = 0.0,
                        individual_deductible_max: float = 1000.0,
                        family_deductible_max: float = 2000.0,
                        individual_oop_max: float = 5000.0,
                        family_oop_max: float = 10000.0):
    """Create a test accumulator snapshot."""
    # Create the member first if it doesn't exist
    conn.execute("""
        INSERT OR IGNORE INTO members (
            member_id, subscriber_id, client_id, first_name, last_name, dob,
            relationship_code, family_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        member_id, member_id.replace('MEM-', 'SUB-'), client_id,
        "Test", "Member", "1980-01-01", "SELF", family_id
    ))

    conn.execute("""
        INSERT INTO accumulator_snapshots (
            member_id, family_id, client_id, plan_id, benefit_year,
            individual_deductible_accum, family_deductible_accum,
            individual_oop_accum, family_oop_accum,
            individual_deductible_met_flag, family_deductible_met_flag,
            individual_oop_met_flag, family_oop_met_flag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        member_id, family_id, client_id, plan_id, 2025,
        individual_deductible_accum, family_deductible_accum,
        individual_oop_accum, family_oop_accum,
        0, 0, 0, 0
    ))

    # Update plan with the max values
    conn.execute("""
        UPDATE benefit_plans
        SET individual_deductible = ?, family_deductible = ?,
            individual_oop_max = ?, family_oop_max = ?
        WHERE plan_id = ?
    """, (individual_deductible_max, family_deductible_max,
          individual_oop_max, family_oop_max, plan_id))


def test_negative_accumulator_creates_medium_severity_issue(temp_db_path):
    """Test that negative accumulator anomalies create MEDIUM severity issues."""
    conn = get_connection(temp_db_path)
    try:
        # Setup test data
        conn.execute("INSERT INTO clients (client_code, client_name) VALUES (?, ?)", ("TEST", "Test Client"))
        conn.execute("""
            INSERT INTO benefit_plans (plan_code, plan_name, plan_type, client_id, benefit_year,
                                     individual_deductible, family_deductible,
                                     individual_oop_max, family_oop_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("TESTPLAN", "Test Plan", "PPO", 1, 2025, 1000.0, 2000.0, 5000.0, 10000.0))
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create snapshot with negative accumulator
        create_test_snapshot(
            conn=conn,
            member_id="MEM-NEG",
            family_id="FAM-NEG",
            client_id=1,
            plan_id=plan_id,
            individual_deductible_accum=-50.0  # Negative value
        )
        conn.commit()

        # Check snapshots exist before anomaly detection
        snapshots = fetch_all("""
            SELECT * FROM accumulator_snapshots
            WHERE member_id = 'MEM-NEG'
        """, db_path=temp_db_path)
        assert len(snapshots) >= 1, f"No snapshots found for member MEM-NEG, snapshots: {snapshots}"

        # Debug: Check what detect_accumulator_anomalies sees
        snapshot_rows = conn.execute(
            """
            SELECT
                s.snapshot_id,
                s.member_id,
                s.family_id,
                s.client_id,
                s.plan_id,
                s.benefit_year,
                s.individual_deductible_accum,
                s.family_deductible_accum,
                s.individual_oop_accum,
                s.family_oop_accum,
                p.individual_deductible,
                p.family_deductible,
                p.individual_oop_max,
                p.family_oop_max
            FROM accumulator_snapshots s
            JOIN benefit_plans p
              ON s.plan_id = p.plan_id
            """
        ).fetchall()
        print(f"Anomaly detection found {len(snapshot_rows)} snapshots")
        for row in snapshot_rows:
            print(f"  Snapshot: {dict(row)}")

        # Run anomaly detection
        anomalies_created = detect_accumulator_anomalies(conn)
        conn.commit()  # Commit the issue creation
        print(f"Anomalies created: {anomalies_created}")

        # Verify anomaly was created
        assert anomalies_created >= 1, f"Expected >= 1 anomalies, got {anomalies_created}"

        # Check that the issue has MEDIUM severity
        issue = fetch_one("""
            SELECT severity, issue_subtype
            FROM data_quality_issues
            WHERE issue_subtype = 'NEGATIVE_ACCUMULATOR'
            AND entity_key LIKE 'MEM-NEG|%|NEGATIVE_ACCUMULATOR'
        """, db_path=temp_db_path)

        assert issue is not None
        assert issue['severity'] == 'MEDIUM'
        assert issue['issue_subtype'] == 'NEGATIVE_ACCUMULATOR'

    finally:
        conn.close()


def test_oop_exceeds_max_creates_critical_severity_issue(temp_db_path):
    """Test that OOP exceeding max creates CRITICAL severity issues."""
    conn = get_connection(temp_db_path)
    try:
        # Setup test data
        conn.execute("INSERT INTO clients (client_code, client_name) VALUES (?, ?)", ("TEST", "Test Client"))
        conn.execute("""
            INSERT INTO benefit_plans (plan_code, plan_name, plan_type, client_id, benefit_year,
                                     individual_deductible, family_deductible,
                                     individual_oop_max, family_oop_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("TESTPLAN", "Test Plan", "PPO", 1, 2025, 1000.0, 2000.0, 5000.0, 10000.0))
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create snapshot with OOP exceeding max
        create_test_snapshot(
            conn=conn,
            member_id="MEM-OOP",
            family_id="FAM-OOP",
            client_id=1,
            plan_id=plan_id,
            individual_oop_accum=6000.0,  # Exceeds 5000.0 max
            individual_oop_max=5000.0
        )
        conn.commit()

        # Run anomaly detection
        anomalies_created = detect_accumulator_anomalies(conn)
        conn.commit()  # Commit the issue creation

        # Verify anomaly was created
        assert anomalies_created >= 1

        # Check that the issue has CRITICAL severity
        issue = fetch_one("""
            SELECT severity, issue_subtype
            FROM data_quality_issues
            WHERE issue_subtype = 'IND_OOP_EXCEEDS_MAX'
            AND entity_key LIKE 'MEM-OOP|%|IND_OOP_EXCEEDS_MAX'
        """, db_path=temp_db_path)

        assert issue is not None
        assert issue['severity'] == 'CRITICAL'
        assert issue['issue_subtype'] == 'IND_OOP_EXCEEDS_MAX'

    finally:
        conn.close()


def test_family_rollup_mismatch_creates_medium_severity_issue(temp_db_path):
    """Test that family rollup mismatch creates MEDIUM severity issues."""
    conn = get_connection(temp_db_path)
    try:
        # Setup test data
        conn.execute("INSERT INTO clients (client_code, client_name) VALUES (?, ?)", ("TEST", "Test Client"))
        conn.execute("""
            INSERT INTO benefit_plans (plan_code, plan_name, plan_type, client_id, benefit_year,
                                     individual_deductible, family_deductible,
                                     individual_oop_max, family_oop_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("TESTPLAN", "Test Plan", "PPO", 1, 2025, 1000.0, 2000.0, 5000.0, 10000.0))
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create family with two members
        create_test_snapshot(
            conn=conn,
            member_id="MEM1-FAM",
            family_id="FAM-ROLLUP",
            client_id=1,
            plan_id=plan_id,
            individual_oop_accum=1000.0,
            family_oop_accum=2500.0  # Should be 1000 + 1000 = 2000, but is 2500
        )

        create_test_snapshot(
            conn=conn,
            member_id="MEM2-FAM",
            family_id="FAM-ROLLUP",
            client_id=1,
            plan_id=plan_id,
            individual_oop_accum=1000.0,
            family_oop_accum=2500.0  # Same family accumulator value
        )
        conn.commit()

        # Run anomaly detection
        anomalies_created = detect_accumulator_anomalies(conn)
        conn.commit()  # Commit the issue creation

        # Verify anomaly was created
        assert anomalies_created >= 1

        # Check that the issue has MEDIUM severity
        issues = fetch_all("""
            SELECT severity, issue_subtype
            FROM data_quality_issues
            WHERE issue_subtype = 'FAMILY_ROLLUP_MISMATCH'
        """, db_path=temp_db_path)

        assert len(issues) >= 1
        # Should have MEDIUM severity
        for issue in issues:
            assert issue['severity'] == 'MEDIUM'
            assert issue['issue_subtype'] == 'FAMILY_ROLLUP_MISMATCH'

    finally:
        conn.close()


def test_negative_accumulator_does_not_auto_create_support_case(temp_db_path):
    """Test that MEDIUM severity negative accumulator issues do not auto-create support cases."""
    from src.issues.support_case_service import create_support_cases_from_open_issues

    conn = get_connection(temp_db_path)
    try:
        # Setup test data
        conn.execute("INSERT INTO clients (client_code, client_name) VALUES (?, ?)", ("TEST", "Test Client"))
        conn.execute("""
            INSERT INTO benefit_plans (plan_code, plan_name, plan_type, client_id, benefit_year,
                                     individual_deductible, family_deductible,
                                     individual_oop_max, family_oop_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("TESTPLAN", "Test Plan", "PPO", 1, 2025, 1000.0, 2000.0, 5000.0, 10000.0))
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create snapshot with negative accumulator
        create_test_snapshot(
            conn=conn,
            member_id="MEM-NEG-NO-CASE",
            family_id="FAM-NEG-NO-CASE",
            client_id=1,
            plan_id=plan_id,
            individual_deductible_accum=-25.0
        )
        conn.commit()

        # Run anomaly detection
        detect_accumulator_anomalies(conn)
        conn.commit()  # Commit the issue creation

        # Try to create support cases
        cases_created = create_support_cases_from_open_issues(conn)
        conn.commit()  # Commit any case creation

        # Verify no cases were created for negative accumulator
        assert cases_created == 0

        # Verify issue exists but no case
        issue = fetch_one("""
            SELECT issue_id
            FROM data_quality_issues
            WHERE issue_subtype = 'NEGATIVE_ACCUMULATOR'
            AND entity_key LIKE 'MEM-NEG-NO-CASE|%'
        """, db_path=temp_db_path)

        assert issue is not None

        # Verify no support case was created
        support_case = fetch_one("""
            SELECT case_id
            FROM support_cases
            WHERE issue_id = ?
        """, (issue['issue_id'],), db_path=temp_db_path)

        assert support_case is None

    finally:
        conn.close()


def test_critical_oop_anomaly_does_auto_create_support_case(temp_db_path):
    """Test that CRITICAL severity OOP anomalies do auto-create support cases."""
    from src.issues.support_case_service import create_support_cases_from_open_issues

    conn = get_connection(temp_db_path)
    try:
        # Setup test data
        conn.execute("INSERT INTO clients (client_code, client_name) VALUES (?, ?)", ("TEST", "Test Client"))
        conn.execute("""
            INSERT INTO benefit_plans (plan_code, plan_name, plan_type, client_id, benefit_year,
                                     individual_deductible, family_deductible,
                                     individual_oop_max, family_oop_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("TESTPLAN", "Test Plan", "PPO", 1, 2025, 1000.0, 2000.0, 5000.0, 10000.0))
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create snapshot with OOP exceeding max (CRITICAL)
        create_test_snapshot(
            conn=conn,
            member_id="MEM-CRIT-CASE",
            family_id="FAM-CRIT-CASE",
            client_id=1,
            plan_id=plan_id,
            family_oop_accum=15000.0,  # Exceeds 10000.0 max
            family_oop_max=10000.0
        )
        conn.commit()

        # Run anomaly detection
        detect_accumulator_anomalies(conn)
        conn.commit()  # Commit the issue creation

        # Create support cases
        cases_created = create_support_cases_from_open_issues(conn)
        conn.commit()  # Commit the case creation

        # Verify case was created for critical anomaly
        assert cases_created >= 1

        # Verify issue and case exist
        issue = fetch_one("""
            SELECT issue_id
            FROM data_quality_issues
            WHERE issue_subtype = 'FAM_OOP_EXCEEDS_MAX'
            AND entity_key LIKE 'MEM-CRIT-CASE|%'
        """, db_path=temp_db_path)

        assert issue is not None

        # Verify support case was created
        support_case = fetch_one("""
            SELECT case_id, severity
            FROM support_cases
            WHERE issue_id = ?
        """, (issue['issue_id'],), db_path=temp_db_path)

        assert support_case is not None
        assert support_case['severity'] == 'CRITICAL'

    finally:
        conn.close()


def test_anomaly_deduplication_still_works(temp_db_path):
    """Test that running anomaly detection multiple times doesn't create duplicate issues."""
    conn = get_connection(temp_db_path)
    try:
        # Setup test data
        conn.execute("INSERT INTO clients (client_code, client_name) VALUES (?, ?)", ("TEST", "Test Client"))
        conn.execute("""
            INSERT INTO benefit_plans (plan_code, plan_name, plan_type, client_id, benefit_year,
                                     individual_deductible, family_deductible,
                                     individual_oop_max, family_oop_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("TESTPLAN", "Test Plan", "PPO", 1, 2025, 1000.0, 2000.0, 5000.0, 10000.0))
        plan_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create snapshot with negative accumulator
        create_test_snapshot(
            conn=conn,
            member_id="MEM-DEDUPE",
            family_id="FAM-DEDUPE",
            client_id=1,
            plan_id=plan_id,
            individual_oop_accum=-10.0
        )
        conn.commit()

        # Run anomaly detection multiple times
        anomalies_1 = detect_accumulator_anomalies(conn)
        conn.commit()  # Commit after first run
        anomalies_2 = detect_accumulator_anomalies(conn)
        anomalies_3 = detect_accumulator_anomalies(conn)

        # First run should create anomaly, subsequent runs should not
        assert anomalies_1 >= 1
        assert anomalies_2 == 0
        assert anomalies_3 == 0

        # Verify only one issue exists
        issues = fetch_all("""
            SELECT issue_id
            FROM data_quality_issues
            WHERE issue_subtype = 'NEGATIVE_ACCUMULATOR'
            AND entity_key LIKE 'MEM-DEDUPE|%'
        """, db_path=temp_db_path)

        assert len(issues) == 1

    finally:
        conn.close()