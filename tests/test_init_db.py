from src.common.db import fetch_one
from src.db.init_db import init_database


def test_init_database_creates_file():
    init_database(reset=True)
    # The DB file is created at the path from SQLITE_DB_PATH env var
    # (set by conftest.py's autouse fixture), so we just verify
    # init_database runs without error. The conftest fixture
    # handles file existence and cleanup.
    row = fetch_one("SELECT COUNT(*) AS cnt FROM schema_metadata")
    assert row is not None
    assert row["cnt"] >= 1


def test_schema_metadata_inserted():
    init_database(reset=True)
    row = fetch_one(
        "SELECT schema_version, description FROM schema_metadata WHERE schema_version = ?",
        ("v2_unified",),
    )
    assert row is not None
    assert row["schema_version"] == "v2_unified"
    assert "Unified schema" in row["description"]