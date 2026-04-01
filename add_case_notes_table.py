"""One-time migration: add case_notes table if missing."""
from src.common.db import db_session

def migrate():
    with db_session() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS case_notes (
                note_id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL,
                note TEXT NOT NULL,
                author TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (case_id) REFERENCES support_cases(case_id)
            )
        """)
    print("✅ case_notes table ensured.")

if __name__ == "__main__":
    migrate()