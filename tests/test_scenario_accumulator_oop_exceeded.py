from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from src.common.db import get_connection
from src.db.init_db import init_database


@pytest.fixture
def temp_db(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_ops.db"
        monkeypatch.setenv("SQLITE_DB_PATH", str(db_path))
        initialize_test_db(str(db_path))
        yield str(db_path)


def initialize_test_db(db_path: str):
    """Initialize DB with schema and minimal seed data for scenario tests."""
    os.environ["SQLITE_DB_PATH"] = db_path
    initialize_database_only()
    seed_minimal_data(db_path)


def initialize_database_only():
    initialize_db = init_database
    initialize_db(reset=True)


def seed_minimal_data(db_path: str):
    """Seed just enough data for the accumulator scenario to run."""
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


SCENARIO_MODULE = "src.scenarios.scenario_accumulator_oop_exceeded"
SCENARIO_ISSUE_CODE = "ACCUMULATOR_EXCEEDS_OOP_MAX"
SCENARIO_MEMBER_ID = "SCN_ACC_MEMBER_0001"
SCENARIO_CLAIM_ID = "SCN-ACC-OOP-0001"
SCENARIO_BENEFIT_YEAR = 2024
SCENARIO_ENTITY_NAME = "accumulator_snapshots"
SCENARIO_ENTITY_KEY = f"{SCENARIO_MEMBER_ID}:{SCENARIO_BENEFIT_YEAR}:INDIVIDUAL_OOP"


def run_scenario(db_path: str) -> None:
    """Run scenario as subprocess, propagating SQLITE_DB_PATH."""
    env = os.environ.copy()
    env["SQLITE_DB_PATH"] = db_path
    result = subprocess.run(
        [sys.executable, "-m", SCENARIO_MODULE],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, (
        f"Scenario run failed.\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def fetch_one(conn, query: str, params: tuple = ()):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchone()


def fetch_all(conn, query: str, params: tuple = ()):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchall()


def get_latest_issue(conn):
    return fetch_one(conn, """
        SELECT issue_id, issue_code, severity, status, client_id, vendor_id,
               member_id, claim_record_id, entity_name, entity_key, issue_message
        FROM data_quality_issues
        WHERE issue_code = ? AND member_id = ? AND entity_name = ? AND entity_key = ?
        ORDER BY issue_id DESC LIMIT 1
    """, (SCENARIO_ISSUE_CODE, SCENARIO_MEMBER_ID, SCENARIO_ENTITY_NAME, SCENARIO_ENTITY_KEY))


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


def get_claim(conn):
    return fetch_one(conn, """
        SELECT claim_record_id, claim_id, line_id, member_id, client_id,
               plan_id, vendor_id, service_date, member_responsibility,
               coinsurance_amount, claim_status, source_file_id
        FROM claims WHERE claim_id = ?
        ORDER BY claim_record_id DESC LIMIT 1
    """, (SCENARIO_CLAIM_ID,))


def get_accumulator_transactions(conn):
    return fetch_all(conn, """
        SELECT accumulator_txn_id, member_id, plan_id, claim_record_id,
               benefit_year, accumulator_type, delta_amount, service_date, source_type
        FROM accumulator_transactions
        WHERE member_id = ? AND benefit_year = ? AND accumulator_type = 'INDIVIDUAL_OOP'
        ORDER BY accumulator_txn_id
    """, (SCENARIO_MEMBER_ID, SCENARIO_BENEFIT_YEAR))


def get_latest_snapshot_with_plan(conn):
    return fetch_one(conn, """
        SELECT s.snapshot_id, s.member_id, s.plan_id, s.benefit_year,
               s.individual_oop_accum, s.individual_oop_met_flag,
               p.individual_oop_max
        FROM accumulator_snapshots s
        JOIN benefit_plans p ON s.plan_id = p.plan_id
        WHERE s.member_id = ? AND s.benefit_year = ?
        ORDER BY s.snapshot_id DESC LIMIT 1
    """, (SCENARIO_MEMBER_ID, SCENARIO_BENEFIT_YEAR))


def get_open_issue_count(conn):
    row = fetch_one(conn, """
        SELECT COUNT(*) AS cnt FROM data_quality_issues
        WHERE issue_code = ? AND status = 'OPEN'
          AND member_id = ? AND entity_name = ? AND entity_key = ?
    """, (SCENARIO_ISSUE_CODE, SCENARIO_MEMBER_ID, SCENARIO_ENTITY_NAME, SCENARIO_ENTITY_KEY))
    return row["cnt"]


def get_case_count_for_issue(conn, issue_id):
    row = fetch_one(conn, "SELECT COUNT(*) AS cnt FROM support_cases WHERE issue_id = ?", (issue_id,))
    return row["cnt"]


def get_sla_count_for_case(conn, case_id):
    row = fetch_one(conn, "SELECT COUNT(*) AS cnt FROM sla_tracking WHERE case_id = ?", (case_id,))
    return row["cnt"]


def test_scenario_accumulator_oop_exceeded_creates_expected_operational_records(temp_db):
    run_scenario(temp_db)

    conn = get_connection(temp_db)
    try:
        issue = get_latest_issue(conn)
        assert issue is not None, "Expected ACCUMULATOR_EXCEEDS_OOP_MAX issue to be created."
        assert issue["issue_code"] == SCENARIO_ISSUE_CODE
        assert issue["severity"] == "HIGH"
        assert issue["status"] == "OPEN"
        assert issue["member_id"] == SCENARIO_MEMBER_ID

        claim = get_claim(conn)
        assert claim is not None
        assert claim["claim_id"] == SCENARIO_CLAIM_ID
        assert claim["member_id"] == SCENARIO_MEMBER_ID
        assert claim["claim_status"] == "PAID"
        assert claim["claim_record_id"] == issue["claim_record_id"]

        txns = get_accumulator_transactions(conn)
        assert len(txns) >= 2
        source_types = {row["source_type"] for row in txns}
        assert "SCENARIO_BASELINE" in source_types
        assert "CLAIM" in source_types

        snapshot = get_latest_snapshot_with_plan(conn)
        assert snapshot is not None
        assert snapshot["individual_oop_accum"] > snapshot["individual_oop_max"]
        assert snapshot["individual_oop_met_flag"] == 1

        case = get_case_for_issue(conn, issue["issue_id"])
        assert case is not None
        assert case["assigned_team"] == "ops_recon_queue"
        assert case["status"] == "OPEN"
        assert case["priority"] == "HIGH"

        sla = get_sla_for_case(conn, case["case_id"])
        assert sla is not None
        assert sla["target_hours"] == 8
        assert sla["status"] in {"OPEN", "AT_RISK", "BREACHED"}
    finally:
        conn.close()


def test_scenario_accumulator_oop_exceeded_is_idempotent_for_same_open_incident(temp_db):
    run_scenario(temp_db)
    run_scenario(temp_db)

    conn = get_connection(temp_db)
    try:
        issue = get_latest_issue(conn)
        assert issue is not None

        assert get_open_issue_count(conn) == 1
        case = get_case_for_issue(conn, issue["issue_id"])
        assert case is not None
        assert get_case_count_for_issue(conn, issue["issue_id"]) == 1

        sla = get_sla_for_case(conn, case["case_id"])
        assert sla is not None
        assert get_sla_count_for_case(conn, case["case_id"]) == 1

        snapshot = get_latest_snapshot_with_plan(conn)
        assert snapshot is not None
        assert snapshot["individual_oop_accum"] > snapshot["individual_oop_max"]
    finally:
        conn.close()