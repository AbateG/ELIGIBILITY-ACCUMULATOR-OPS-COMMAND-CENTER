"""
Streamlit Cloud entrypoint.

This file lives at the repo root so that Python's sys.path includes
the repo root, making `from src.common.db import ...` resolve correctly.
It also ensures the database is initialised before the UI loads.
"""

import sys
from pathlib import Path

# ── 1. Guarantee the repo root is on sys.path ──────────────────────────
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── 2. Initialise the database if it doesn't exist ─────────────────────
from config.settings import DB_PATH, SCHEMA_PATH  # noqa: E402


def _ensure_db():
    """Create the SQLite DB and run schema + seed if tables are missing."""
    import sqlite3

    db_exists = DB_PATH.exists()
    # Ensure the db/ directory exists (Streamlit Cloud starts fresh)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    try:
        # Check whether the core table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='inbound_files'"
        )
        table_exists = cur.fetchone() is not None

        if not table_exists:
            # Run the full schema
            if SCHEMA_PATH.exists():
                conn.executescript(SCHEMA_PATH.read_text())
                print(f"[startup] Schema applied from {SCHEMA_PATH}")
            else:
                print(f"[startup] WARNING: schema file not found at {SCHEMA_PATH}")

            # Run seed data if available
            seed_path = SCHEMA_PATH.parent / "seed_data.sql"
            if seed_path.exists():
                conn.executescript(seed_path.read_text())
                print(f"[startup] Seed data applied from {seed_path}")
        else:
            print("[startup] Database already initialised.")
    finally:
        conn.close()


_ensure_db()

# ── 3. Launch the real app via Streamlit's multi-page navigation ────────
import streamlit as st  # noqa: E402

home = st.Page("src/app/Home.py", title="Home", icon="🏠", default=True)

pages = [
    home,
    st.Page("src/app/pages/File_Monitoring.py", title="File Monitoring", icon="📁"),
    st.Page("src/app/pages/File_Detail.py", title="File Detail", icon="📄"),
    st.Page("src/app/pages/Processing_Run_Dashboard.py", title="Processing Runs", icon="⚙️"),
    st.Page("src/app/pages/Processing_Run_Detail.py", title="Run Detail", icon="🔍"),
    st.Page("src/app/pages/Accumulator_Reconciliation.py", title="Accumulator Reconciliation", icon="💰"),
    st.Page("src/app/pages/Issue_Triage.py", title="Issue Triage", icon="🚨"),
    st.Page("src/app/pages/Member_Timeline.py", title="Member Timeline", icon="👤"),
    st.Page("src/app/pages/Support_Case_Detail.py", title="Support Case Detail", icon="🎫"),
    st.Page("src/app/pages/SLA_Detail.py", title="SLA Detail", icon="⏱️"),
    st.Page("src/app/pages/Scenario_Control_Center.py", title="Scenario Control", icon="🎬"),
    st.Page("src/app/pages/Investigation_Playbooks.py", title="Investigation Playbooks", icon="📋"),
    st.Page("src/app/pages/SQL_Query_Workbench.py", title="SQL Workbench", icon="🗃️"),
]

pg = st.navigation(pages)
pg.run()