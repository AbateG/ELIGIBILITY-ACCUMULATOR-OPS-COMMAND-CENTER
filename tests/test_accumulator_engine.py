import sqlite3
import tempfile
from pathlib import Path

import pytest

from src.db.init_db import init_database as init_db_main
from src.data_generation.generate_seed_data import seed_reference_data as seed_main


@pytest.fixture
def temp_db(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_ops.db"
        monkeypatch.setenv("SQLITE_DB_PATH", str(db_path))
        init_db_main()
        seed_main()
        yield str(db_path)


def get_table_columns(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def test_accumulator_tables_exist(temp_db):
    conn = sqlite3.connect(temp_db)
    try:
        cur = conn.cursor()

        for table_name in ["accumulator_transactions", "accumulator_snapshots"]:
            cur.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name=?
                """,
                (table_name,),
            )
            assert cur.fetchone() is not None
    finally:
        conn.close()


def test_accumulator_snapshots_table_has_expected_structure(temp_db):
    conn = sqlite3.connect(temp_db)
    try:
        columns = get_table_columns(conn, "accumulator_snapshots")
        assert len(columns) > 0
        assert "member_id" in columns or "accumulator_id" in columns
    finally:
        conn.close()


def test_data_quality_issues_table_has_required_columns(temp_db):
    conn = sqlite3.connect(temp_db)
    try:
        columns = get_table_columns(conn, "data_quality_issues")
        assert "issue_code" in columns
        assert "issue_description" in columns
    finally:
        conn.close()


def test_oop_exceed_issue_can_be_recorded(temp_db):
    conn = sqlite3.connect(temp_db)
    try:
        columns = get_table_columns(conn, "data_quality_issues")
        cur = conn.cursor()

        insert_values = {
            "issue_code": "ACCUMULATOR_EXCEEDS_OOP_MAX",
            "issue_type": "ACCUMULATOR",
            "severity": "CRITICAL",
            "status": "OPEN",
            "client_id": 1,
            "vendor_id": 1,
            "member_id": 1001,
            "issue_description": "Member accumulator exceeded OOP max after claim application",
            "created_at": "2025-01-01 00:00:00",
            "updated_at": "2025-01-01 00:00:00",
        }

        valid_insert_values = {k: v for k, v in insert_values.items() if k in columns}

        col_list = ", ".join(valid_insert_values.keys())
        placeholders = ", ".join(["?"] * len(valid_insert_values))

        cur.execute(
            f"""
            INSERT INTO data_quality_issues ({col_list})
            VALUES ({placeholders})
            """,
            tuple(valid_insert_values.values()),
        )
        conn.commit()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM data_quality_issues
            WHERE issue_code = 'ACCUMULATOR_EXCEEDS_OOP_MAX'
            """
        )
        count = cur.fetchone()[0]
        assert count >= 1
    finally:
        conn.close()