from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from src.common.db import get_connection
from src.db.init_db import init_database


SCENARIO_MODULE = "src.scenarios.scenario_family_rollup_discrepancy"
SCENARIO_ISSUE_CODE = "FAMILY_ROLLUP_DISCREPANCY"
SCENARIO_FAMILY_ID = "SCN_FAMILY_0001"
SCENARIO_SUBSCRIBER_MEMBER_ID = "SCN_FAM_SUB_0001"
SCENARIO_DEPENDENT_MEMBER_ID = "SCN_FAM_DEP_0001"
SCENARIO_BENEFIT_YEAR = 2024
SCENARIO_SUB_CLAIM_ID = "SCN-FAM-ROLLUP-SUB-0001"
SCENARIO_DEP_CLAIM_ID = "SCN-FAM-ROLLUP-DEP-0001"
SCENARIO_ENTITY_NAME = "accumulator_snapshots"
SCENARIO_ENTITY_KEY = f"{SCENARIO_FAMILY_ID}:{SCENARIO_BENEFIT_YEAR}:FAMILY_OOP"


@pytest.fixture
def temp_db(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_ops.db"
        monkeypatch.setenv("SQLITE_DB_PATH", str(db_path))
        init_database(reset=True)

        from src.common.db import db_session
        with db_session() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO clients (client_id, client_code, client_name, active_flag)
                VALUES (1, 'CASCADE', 'Cascade Health Alliance', 1)
            """)
            conn.execute("""
                INSERT OR IGNORE INTO vendors (vendor_id, vendor_code, vendor_name, vendor_type, active_flag)
                VALUES (1, 'MEDIPROC', 'MediProcess Solutions', 'TPA', 1)
            """)
            conn.execute("""
                INSERT OR IGNORE INTO benefit_plans (
                    plan_id, plan_code, plan_name, plan_type, client_id, benefit_year,
                    individual_deductible, family_deductible,
                    individual_oop_max, family_oop_max,
                    coinsurance_rate, active_flag
                )
                VALUES (1, 'PLN-001', 'Cascade Gold PPO', 'PPO', 1, 2025,
                        1500.00, 3000.00, 6000.00, 12000.00, 0.20, 1)
            """)

        yield str(db_path)


def run_scenario(db_path: str) -> None:
    env = os.environ.copy()
    env["SQLITE_DB_PATH"] = db_path
    result = subprocess.run(
        [sys.executable, "-m", SCENARIO_MODULE],
        capture_output=True, text=True, check=False, env=env,
    )
    assert result.returncode == 0, (
        f"Scenario run failed.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def fetch_one(conn, query, params=()):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchone()


def fetch_all(conn, query, params=()):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchall()


def get_latest_issue(conn):
    return fetch_one(conn, """
        SELECT issue_id, issue_code, severity, status, client_id, vendor_id,
               member_id, claim_record_id, entity_name, entity_key, issue_message
        FROM data_quality_issues
        WHERE issue_code = ? AND entity_name = ? AND entity_key = ?
        ORDER BY issue_id DESC LIMIT 1
    """, (SCENARIO_ISSUE_CODE, SCENARIO_ENTITY_NAME, SCENARIO_ENTITY_KEY))


def get_case_for_issue(conn, issue_id):
    return fetch_one(conn, """
        SELECT case_id, issue_id, case_type, priority, status,
               assigned_team, short_description, description
        FROM support_cases WHERE issue_id = ?
        ORDER BY case_id DESC LIMIT 1
    """, (issue_id,))


def get_sla_for_case(conn, case_id):
    return fetch_one(conn, """
        SELECT sla_id, case_id, sla_type, target_hours, status, is_at_risk, is_breached
        FROM sla_tracking WHERE case_id = ?
        ORDER BY sla_id DESC LIMIT 1
    """, (case_id,))


def get_family_claims(conn):
    return fetch_all(conn, """
        SELECT claim_record_id, claim_id, member_id, subscriber_id,
               client_id, plan_id, vendor_id, service_date,
               member_responsibility, claim_status
        FROM claims WHERE claim_id IN (?, ?)
        ORDER BY claim_record_id ASC
    """, (SCENARIO_SUB_CLAIM_ID, SCENARIO_DEP_CLAIM_ID))


def get_family_transactions(conn):
    return fetch_all(conn, """
        SELECT accumulator_txn_id, member_id, family_id, plan_id,
               claim_record_id, benefit_year, accumulator_type,
               delta_amount, source_type
        FROM accumulator_transactions
        WHERE family_id = ? AND benefit_year = ? AND accumulator_type = 'INDIVIDUAL_OOP'
        ORDER BY accumulator_txn_id ASC
    """, (SCENARIO_FAMILY_ID, SCENARIO_BENEFIT_YEAR))


def get_latest_family_snapshot(conn):
    return fetch_one(conn, """
        SELECT snapshot_id, member_id, family_id, client_id, plan_id,
               benefit_year, individual_oop_accum, family_oop_accum, snapshot_ts
        FROM accumulator_snapshots
        WHERE family_id = ? AND member_id = ? AND benefit_year = ?
        ORDER BY snapshot_id DESC LIMIT 1
    """, (SCENARIO_FAMILY_ID, SCENARIO_SUBSCRIBER_MEMBER_ID, SCENARIO_BENEFIT_YEAR))


def get_expected_vs_actual_family_rollup(conn):
    return fetch_one(conn, """
        SELECT t.family_id, t.plan_id, t.benefit_year,
               SUM(t.delta_amount) AS expected_family_oop,
               s.family_oop_accum AS actual_family_oop,
               SUM(t.delta_amount) - s.family_oop_accum AS discrepancy_amount
        FROM accumulator_transactions t
        JOIN accumulator_snapshots s
          ON s.family_id = t.family_id AND s.plan_id = t.plan_id AND s.benefit_year = t.benefit_year
        WHERE t.family_id = ? AND t.benefit_year = ?
          AND t.accumulator_type = 'INDIVIDUAL_OOP' AND s.member_id = ?
        GROUP BY t.family_id, t.plan_id, t.benefit_year, s.family_oop_accum
        ORDER BY s.snapshot_id DESC LIMIT 1
    """, (SCENARIO_FAMILY_ID, SCENARIO_BENEFIT_YEAR, SCENARIO_SUBSCRIBER_MEMBER_ID))


def get_open_issue_count(conn):
    row = fetch_one(conn, """
        SELECT COUNT(*) AS cnt FROM data_quality_issues
        WHERE issue_code = ? AND status = 'OPEN' AND entity_name = ? AND entity_key = ?
    """, (SCENARIO_ISSUE_CODE, SCENARIO_ENTITY_NAME, SCENARIO_ENTITY_KEY))
    return row["cnt"]


def get_case_count_for_issue(conn, issue_id):
    return fetch_one(conn, "SELECT COUNT(*) AS cnt FROM support_cases WHERE issue_id = ?", (issue_id,))["cnt"]


def get_sla_count_for_case(conn, case_id):
    return fetch_one(conn, "SELECT COUNT(*) AS cnt FROM sla_tracking WHERE case_id = ?", (case_id,))["cnt"]


def test_scenario_family_rollup_discrepancy_creates_expected_operational_records(temp_db):
    run_scenario(temp_db)

    conn = get_connection(temp_db)
    try:
        issue = get_latest_issue(conn)
        assert issue is not None, "Expected FAMILY_ROLLUP_DISCREPANCY issue to be created."
        assert issue["issue_code"] == SCENARIO_ISSUE_CODE
        assert issue["severity"] == "HIGH"
        assert issue["status"] == "OPEN"
        assert issue["entity_name"] == SCENARIO_ENTITY_NAME
        assert issue["entity_key"] == SCENARIO_ENTITY_KEY
        assert issue["member_id"] == SCENARIO_SUBSCRIBER_MEMBER_ID

        claims = get_family_claims(conn)
        assert len(claims) == 2, "Expected deterministic subscriber and dependent claims to exist."

        claim_ids = {row["claim_id"] for row in claims}
        member_ids = {row["member_id"] for row in claims}
        claim_statuses = {row["claim_status"] for row in claims}

        assert SCENARIO_SUB_CLAIM_ID in claim_ids
        assert SCENARIO_DEP_CLAIM_ID in claim_ids
        assert SCENARIO_SUBSCRIBER_MEMBER_ID in member_ids
        assert SCENARIO_DEPENDENT_MEMBER_ID in member_ids
        assert claim_statuses == {"PAID"}

        txns = get_family_transactions(conn)
        assert len(txns) >= 2, "Expected at least two family member accumulator transactions."

        txn_member_ids = {row["member_id"] for row in txns}
        source_types = {row["source_type"] for row in txns}

        assert SCENARIO_SUBSCRIBER_MEMBER_ID in txn_member_ids
        assert SCENARIO_DEPENDENT_MEMBER_ID in txn_member_ids
        assert "CLAIM" in source_types

        snapshot = get_latest_family_snapshot(conn)
        assert snapshot is not None, "Expected family accumulator snapshot to exist."
        assert snapshot["family_id"] == SCENARIO_FAMILY_ID
        assert snapshot["member_id"] == SCENARIO_SUBSCRIBER_MEMBER_ID
        assert snapshot["benefit_year"] == SCENARIO_BENEFIT_YEAR

        rollup = get_expected_vs_actual_family_rollup(conn)
        assert rollup is not None, "Expected rollup reconciliation result to be queryable."
        assert float(rollup["expected_family_oop"]) > float(rollup["actual_family_oop"]), (
            "Expected family rollup scenario to have actual family OOP below summed member OOP."
        )
        assert float(rollup["discrepancy_amount"]) > 0.0, (
            "Expected positive discrepancy amount for family rollup scenario."
        )

        case = get_case_for_issue(conn, issue["issue_id"])
        assert case is not None, "Expected support case to be created from issue."
        assert case["issue_id"] == issue["issue_id"]
        assert case["assigned_team"] == "ops_recon_queue"
        assert case["status"] == "OPEN"
        assert case["priority"] == "HIGH"

        sla = get_sla_for_case(conn, case["case_id"])
        assert sla is not None, "Expected SLA to be created for family rollup support case."
        assert sla["case_id"] == case["case_id"]
        assert sla["target_hours"] == 24
        assert sla["status"] in {"OPEN", "AT_RISK", "BREACHED"}

    finally:
        conn.close()


def test_scenario_family_rollup_discrepancy_is_idempotent_for_same_open_incident(temp_db):
    run_scenario(temp_db)
    run_scenario(temp_db)

    conn = get_connection(temp_db)
    try:
        issue = get_latest_issue(conn)
        assert issue is not None, "Expected family rollup discrepancy issue to exist after reruns."

        open_issue_count = get_open_issue_count(conn)
        assert open_issue_count == 1, (
            f"Expected exactly 1 open family rollup discrepancy issue, found {open_issue_count}."
        )

        case = get_case_for_issue(conn, issue["issue_id"])
        assert case is not None, "Expected support case to exist for deterministic issue."

        case_count = get_case_count_for_issue(conn, issue["issue_id"])
        assert case_count == 1, (
            f"Expected exactly 1 support case for issue_id={issue['issue_id']}, found {case_count}."
        )

        sla = get_sla_for_case(conn, case["case_id"])
        assert sla is not None, "Expected SLA to exist for deterministic support case."

        sla_count = get_sla_count_for_case(conn, case["case_id"])
        assert sla_count == 1, (
            f"Expected exactly 1 SLA for case_id={case['case_id']}, found {sla_count}."
        )

        rollup = get_expected_vs_actual_family_rollup(conn)
        assert rollup is not None
        assert float(rollup["expected_family_oop"]) > float(rollup["actual_family_oop"])
        assert float(rollup["discrepancy_amount"]) > 0.0

    finally:
        conn.close()