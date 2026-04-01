import pytest
import os
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

from src.common.db import db_session, fetch_all, fetch_one
from src.sla.sla_service import (
    determine_target_hours,
    create_sla_for_case,
    evaluate_open_slas
)
from src.common.datetime_utils import utc_now_iso, add_hours_iso, hours_elapsed


@pytest.fixture
def test_db():
    # Create a temporary DB file
    db_fd, db_path = tempfile.mkstemp()
    os.close(db_fd)
    db_path = Path(db_path)

    # Initialize schema
    from config.settings import SCHEMA_PATH
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with db_session(db_path) as conn:
        conn.executescript(schema_sql)
        # Ensure support_cases and sla_tracking exist
        conn.execute("DROP TABLE IF EXISTS support_cases")
        conn.execute("""
        CREATE TABLE support_cases (
            case_id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id INTEGER,
            client_id INTEGER,
            vendor_id INTEGER,
            file_id INTEGER,
            run_id INTEGER,
            member_id TEXT,
            claim_record_id INTEGER,
            case_type TEXT NOT NULL,
            priority TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            assigned_team TEXT,
            assigned_to TEXT,
            escalation_level INTEGER NOT NULL DEFAULT 0,
            source_system TEXT NOT NULL DEFAULT 'SYSTEM',
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            root_cause_category TEXT,
            resolution_summary TEXT,
            opened_at TEXT NOT NULL,
            acknowledged_at TEXT,
            resolved_at TEXT,
            closed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(issue_id) REFERENCES data_quality_issues(issue_id),
            FOREIGN KEY(file_id) REFERENCES inbound_files(file_id),
            FOREIGN KEY(run_id) REFERENCES processing_runs(run_id)
        )
        """)
        conn.execute("DROP TABLE IF EXISTS sla_tracking")
        conn.execute("""
        CREATE TABLE sla_tracking (
            sla_id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id INTEGER NOT NULL,
            sla_type TEXT NOT NULL,
            target_hours INTEGER,
            target_due_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            is_at_risk INTEGER NOT NULL DEFAULT 0,
            is_breached INTEGER NOT NULL DEFAULT 0,
            breached_at TEXT,
            last_evaluated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(case_id) REFERENCES support_cases(case_id)
        )
        """)

    yield db_path

    # Cleanup
    db_path.unlink(missing_ok=True)


def insert_test_case(db_path, **kwargs):
    defaults = {
        "issue_id": None,
        "client_id": None,
        "vendor_id": None,
        "file_id": None,
        "run_id": None,
        "member_id": None,
        "claim_record_id": None,
        "case_type": "GENERAL",
        "priority": "MEDIUM",
        "status": "OPEN",
        "assigned_team": "ops_triage_queue",
        "assigned_to": None,
        "escalation_level": 0,
        "source_system": "SYSTEM",
        "title": "Test case",
        "description": "Test description",
        "root_cause_category": None,
        "resolution_summary": None,
        "opened_at": utc_now_iso(),
        "acknowledged_at": None,
        "resolved_at": None,
        "closed_at": None,
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }
    defaults.update(kwargs)

    with db_session(db_path) as conn:
        cursor = conn.execute("""
            INSERT INTO support_cases (
                issue_id, client_id, vendor_id, file_id, run_id, member_id, claim_record_id,
                case_type, priority, status, assigned_team, assigned_to, escalation_level,
                source_system, title, description, root_cause_category, resolution_summary,
                opened_at, acknowledged_at, resolved_at, closed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            defaults["issue_id"], defaults["client_id"], defaults["vendor_id"], defaults["file_id"],
            defaults["run_id"], defaults["member_id"], defaults["claim_record_id"],
            defaults["case_type"], defaults["priority"], defaults["status"], defaults["assigned_team"],
            defaults["assigned_to"], defaults["escalation_level"], defaults["source_system"],
            defaults["title"], defaults["description"], defaults["root_cause_category"],
            defaults["resolution_summary"], defaults["opened_at"], defaults["acknowledged_at"],
            defaults["resolved_at"], defaults["closed_at"], defaults["created_at"], defaults["updated_at"]
        ))
        return cursor.lastrowid


def test_determine_target_hours():
    assert determine_target_hours("CRITICAL", None) == 4
    assert determine_target_hours("HIGH", None) == 8
    assert determine_target_hours("MEDIUM", None) == 24
    assert determine_target_hours("LOW", None) == 72
    assert determine_target_hours("UNKNOWN", None) == 24


def test_determine_target_hours_with_override():
    assert determine_target_hours("HIGH", "MISSING_INBOUND_FILE") == 4
    assert determine_target_hours("HIGH", "CLAIM_INELIGIBLE_MEMBER") == 8
    assert determine_target_hours("HIGH", "ACCUMULATOR_EXCEEDS_OOP_MAX") == 8
    assert determine_target_hours("HIGH", "FAMILY_ROLLUP_DISCREPANCY") == 24
    assert determine_target_hours("HIGH", "DUPLICATE_ELIGIBILITY_RESEND") == 24


def test_create_sla_for_case(test_db):
    case_id = insert_test_case(test_db, priority="HIGH", case_type="GENERAL")

    with db_session(test_db) as conn:
        create_sla_for_case(case_id, "HIGH", "GENERAL", conn=conn)

    with db_session(test_db) as conn:
        sla = conn.execute("SELECT * FROM sla_tracking WHERE case_id = ?", (case_id,)).fetchone()
        assert sla["sla_type"] == "CASE_RESOLUTION"
        assert sla["target_hours"] == 8
        assert sla["status"] == "OPEN"
        assert sla["is_at_risk"] == 0
        assert sla["is_breached"] == 0


def test_create_sla_not_duplicate(test_db):
    case_id = insert_test_case(test_db, priority="HIGH")

    with db_session(test_db) as conn:
        create_sla_for_case(case_id, "HIGH", "GENERAL", conn=conn)
        create_sla_for_case(case_id, "HIGH", "GENERAL", conn=conn)  # Should not create duplicate

    with db_session(test_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM sla_tracking WHERE case_id = ?", (case_id,)).fetchone()[0]
        assert count == 1


def test_evaluate_open_slas_at_risk(test_db):
    # Create a case with SLA that's old enough to be at risk
    opened_at = add_hours_iso(utc_now_iso(), -7)  # 7 hours ago, target 8, so at risk
    case_id = insert_test_case(test_db, priority="HIGH", opened_at=opened_at, created_at=opened_at, updated_at=opened_at)

    with db_session(test_db) as conn:
        create_sla_for_case(case_id, "HIGH", "GENERAL", conn=conn)

        # Manually set created_at to match opened_at
        conn.execute("UPDATE sla_tracking SET created_at = ?, target_due_at = ?, last_evaluated_at = ? WHERE case_id = ?", (opened_at, add_hours_iso(opened_at, 8), utc_now_iso(), case_id))

        updated = evaluate_open_slas(conn=conn)

    assert updated >= 1

    with db_session(test_db) as conn:
        sla = conn.execute("SELECT * FROM sla_tracking WHERE case_id = ?", (case_id,)).fetchone()
        assert sla["status"] == "AT_RISK"
        assert sla["is_at_risk"] == 1


def test_evaluate_open_slas_breached(test_db):
    # Create a case with SLA that's breached
    opened_at = add_hours_iso(utc_now_iso(), -10)  # 10 hours ago, target 8, so breached
    case_id = insert_test_case(test_db, priority="HIGH", opened_at=opened_at, created_at=opened_at, updated_at=opened_at)

    with db_session(test_db) as conn:
        create_sla_for_case(case_id, "HIGH", "GENERAL", conn=conn)

        conn.execute("UPDATE sla_tracking SET created_at = ?, target_due_at = ?, last_evaluated_at = ? WHERE case_id = ?", (opened_at, add_hours_iso(opened_at, 8), utc_now_iso(), case_id))

        updated = evaluate_open_slas(conn=conn)

    assert updated >= 1

    with db_session(test_db) as conn:
        sla = conn.execute("SELECT * FROM sla_tracking WHERE case_id = ?", (case_id,)).fetchone()
        assert sla["status"] == "BREACHED"
        assert sla["is_breached"] == 1
        assert sla["breached_at"] is not None


def test_resolved_case_closes_sla(test_db):
    case_id = insert_test_case(test_db, priority="HIGH", status="RESOLVED", resolved_at=utc_now_iso())

    with db_session(test_db) as conn:
        create_sla_for_case(case_id, "HIGH", "GENERAL", conn=conn)

        updated = evaluate_open_slas(conn=conn)

    assert updated >= 1

    with db_session(test_db) as conn:
        sla = conn.execute("SELECT * FROM sla_tracking WHERE case_id = ?", (case_id,)).fetchone()
        assert sla["status"] == "CLOSED"
        assert sla["is_at_risk"] == 0
        assert sla["is_breached"] == 0