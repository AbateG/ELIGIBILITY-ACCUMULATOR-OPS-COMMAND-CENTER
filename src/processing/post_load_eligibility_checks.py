from datetime import datetime

from src.common.db import db_session, fetch_all
from src.issues.issue_service import create_issue
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.sla.sla_service import evaluate_open_slas


def detect_exact_duplicate_segments(conn) -> int:
    cursor = conn.execute(
        """
        SELECT
            ep.member_id,
            ep.subscriber_id,
            ep.client_id,
            ep.plan_id,
            ep.coverage_start,
            COALESCE(ep.coverage_end, '9999-12-31') AS coverage_end_norm,
            COUNT(*) AS dup_count
        FROM eligibility_periods ep
        GROUP BY
            ep.member_id,
            ep.subscriber_id,
            ep.client_id,
            ep.plan_id,
            ep.coverage_start,
            COALESCE(ep.coverage_end, '9999-12-31')
        HAVING COUNT(*) > 1
        """
    )

    created = 0

    for row in duplicate_rows:
        issue_exists = conn.execute(
            """
            SELECT 1
            FROM data_quality_issues
            WHERE issue_type = 'ELIGIBILITY'
              AND issue_subtype = 'DUPLICATE_ELIGIBILITY_SEGMENT'
              AND member_id = ?
              AND entity_key = ?
            LIMIT 1
            """,
            (
                row["member_id"],
                f"{row['member_id']}|{row['plan_id']}|{row['coverage_start']}|{row['coverage_end_norm']}",
            ),
        ).fetchone()

        if issue_exists:
            continue

        create_issue(
            conn=conn,
            issue_type="ELIGIBILITY",
            issue_subtype="DUPLICATE_ELIGIBILITY_SEGMENT",
            severity="HIGH",
            status="OPEN",
            client_id=row["client_id"],
            member_id=row["member_id"],
            entity_name="eligibility_periods",
            entity_key=f"{row['member_id']}|{row['plan_id']}|{row['coverage_start']}|{row['coverage_end_norm']}",
            issue_description=(
                f"Duplicate eligibility segment detected for member {row['member_id']} "
                f"plan_id={row['plan_id']} coverage_start={row['coverage_start']} "
                f"coverage_end={row['coverage_end_norm']} count={row['dup_count']}"
            ),
        )
        created += 1

    return created


def detect_overlapping_eligibility_periods(conn) -> int:
    overlap_rows = fetch_all(
        """
        SELECT
            a.member_id,
            a.client_id,
            a.plan_id AS plan_id_a,
            b.plan_id AS plan_id_b,
            a.coverage_start AS a_start,
            COALESCE(a.coverage_end, '9999-12-31') AS a_end,
            b.coverage_start AS b_start,
            COALESCE(b.coverage_end, '9999-12-31') AS b_end,
            a.eligibility_id AS elig_id_a,
            b.eligibility_id AS elig_id_b
        FROM eligibility_periods a
        JOIN eligibility_periods b
          ON a.member_id = b.member_id
         AND a.eligibility_id < b.eligibility_id
         AND a.coverage_start <= COALESCE(b.coverage_end, '9999-12-31')
         AND b.coverage_start <= COALESCE(a.coverage_end, '9999-12-31')
        """
    )

    created = 0

    for row in overlap_rows:
        entity_key = f"{row['member_id']}|{row['elig_id_a']}|{row['elig_id_b']}"

        issue_exists = conn.execute(
            """
            SELECT 1
            FROM data_quality_issues
            WHERE issue_type = 'ELIGIBILITY'
              AND issue_subtype = 'OVERLAPPING_COVERAGE'
              AND entity_key = ?
            LIMIT 1
            """,
            (entity_key,),
        ).fetchone()

        if issue_exists:
            continue

        severity = "HIGH"
        subtype = "OVERLAPPING_COVERAGE"

        if row["plan_id_a"] != row["plan_id_b"]:
            subtype = "BAD_PLAN_TRANSFER"
            severity = "HIGH"

        create_issue(
            conn=conn,
            issue_type="ELIGIBILITY",
            issue_subtype=subtype,
            severity=severity,
            status="OPEN",
            client_id=row["client_id"],
            member_id=row["member_id"],
            entity_name="eligibility_periods",
            entity_key=entity_key,
            issue_description=(
                f"Overlapping eligibility detected for member {row['member_id']}: "
                f"segment A ({row['a_start']} to {row['a_end']}, plan {row['plan_id_a']}) "
                f"overlaps segment B ({row['b_start']} to {row['b_end']}, plan {row['plan_id_b']})"
            ),
        )
        created += 1

    return created


def run_post_load_eligibility_checks() -> None:
    with db_session() as conn:
        started_at = datetime.now().isoformat(timespec="seconds")
        cursor = conn.execute(
            """
            INSERT INTO processing_runs (run_type, started_at, run_status)
            VALUES (?, ?, ?)
            """,
            ("ELIGIBILITY_POST_LOAD_CHECK", started_at, "RUNNING"),
        )
        run_id = cursor.lastrowid

        try:
            dup_count = detect_exact_duplicate_segments(conn)
            overlap_count = detect_overlapping_eligibility_periods(conn)
            total_issues = dup_count + overlap_count

            completed_at = datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                UPDATE processing_runs
                SET completed_at = ?, run_status = ?, issue_count = ?
                WHERE run_id = ?
                """,
                (completed_at, "SUCCESS", total_issues, run_id),
            )

            conn.execute(
                """
                INSERT INTO audit_log (event_type, entity_name, entity_key, run_id, actor, event_details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "ELIGIBILITY_POST_LOAD_CHECK_COMPLETED",
                    "eligibility_periods",
                    "post_load_scan",
                    run_id,
                    "system",
                    f"Post-load checks completed: duplicate_segments={dup_count}, overlaps={overlap_count}",
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

    print("Post-load eligibility checks completed.")


if __name__ == "__main__":
    run_post_load_eligibility_checks()