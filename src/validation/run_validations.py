from datetime import datetime

from src.common.db import db_session, fetch_all
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.sla.sla_service import evaluate_open_slas
from src.validation.file_validators import run_file_validations


def run_all_file_validations() -> None:
    file_records = fetch_all(
        """
        SELECT *
        FROM inbound_files
        WHERE processing_status = 'RECEIVED'
        ORDER BY file_id
        """
    )

    if not file_records:
        print("No RECEIVED files to validate.")
        return

    with db_session() as conn:
        for file_record in file_records:
            started_at = datetime.now().isoformat(timespec="seconds")
            cursor = conn.execute(
                """
                INSERT INTO processing_runs (
                    run_type, file_id, started_at, run_status
                )
                VALUES (?, ?, ?, ?)
                """,
                ("VALIDATION", file_record["file_id"], started_at, "RUNNING"),
            )
            run_id = cursor.lastrowid

            try:
                issue_ids = run_file_validations(conn, file_record)

                completed_at = datetime.now().isoformat(timespec="seconds")
                conn.execute(
                    """
                    UPDATE processing_runs
                    SET completed_at = ?, run_status = ?, issue_count = ?
                    WHERE run_id = ?
                    """,
                    (
                        completed_at,
                        "SUCCESS" if not issue_ids else "PARTIAL_SUCCESS",
                        len(issue_ids),
                        run_id,
                    ),
                )

                conn.execute(
                    """
                    INSERT INTO audit_log (event_type, entity_name, entity_key, run_id, file_id, actor, event_details)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "VALIDATION_COMPLETED",
                        "inbound_files",
                        str(file_record["file_id"]),
                        run_id,
                        file_record["file_id"],
                        "system",
                        f"Completed file validation with {len(issue_ids)} issue(s)",
                    ),
                )

                # Create support cases from issues and evaluate SLAs
                create_support_cases_from_open_issues(conn)
                evaluate_open_slas(conn)
            except Exception as exc:
                completed_at = datetime.now().isoformat(timespec="seconds")
                conn.execute(
                    """
                    UPDATE processing_runs
                    SET completed_at = ?, run_status = ?, notes = ?
                    WHERE run_id = ?
                    """,
                    (completed_at, "FAILED", str(exc), run_id),
                )
                raise

    print(f"Validated {len(file_records)} file(s).")


if __name__ == "__main__":
    run_all_file_validations()