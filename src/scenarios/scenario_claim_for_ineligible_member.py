from datetime import datetime, timedelta, UTC

from src.common.db import db_session, fetch_one, execute
from src.issues.run_case_generation import main as run_case_generation_main
from src.sla.sla_service import create_sla_for_case, evaluate_open_slas


SCENARIO_NAME = "CLAIM_INELIGIBLE_MEMBER"
DEFAULT_SEVERITY = "HIGH"
DEFAULT_RUN_TYPE = "CLAIMS_PROCESSING"


def _utcnow_iso():
    return datetime.now(UTC).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")


def _today_date():
    return datetime.now(UTC).date().isoformat()


def _generate_claim_id(member_id, service_date):
    safe_member = str(member_id).replace(" ", "").replace("-", "")
    safe_date = str(service_date).replace("-", "")
    return f"SCNCLM_{safe_member}_{safe_date}"


def _generate_line_id(member_id, service_date):
    safe_member = str(member_id).replace(" ", "").replace("-", "")
    safe_date = str(service_date).replace("-", "")
    return f"LN1_{safe_member}_{safe_date}"


def _get_member_context_with_eligibility():
    return fetch_one(
        """
        SELECT
            ep.member_id,
            ep.subscriber_id,
            ep.client_id,
            ep.plan_id,
            ep.vendor_id,
            ep.coverage_start,
            ep.coverage_end,
            ep.status
        FROM eligibility_periods ep
        ORDER BY
            CASE WHEN ep.coverage_end IS NULL THEN 1 ELSE 0 END,
            ep.coverage_end DESC,
            ep.coverage_start DESC,
            ep.eligibility_id DESC
        LIMIT 1
        """
    )


def _ensure_member_context():
    client = fetch_one("SELECT client_id FROM clients ORDER BY client_id LIMIT 1")

    if not client:
        now_ts = _utcnow_iso()
        with db_session() as conn:
            conn.execute(
                """
                INSERT INTO clients (client_id, client_code, client_name, active_flag, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (1, "SCN_CLIENT", "Scenario Client", 1, now_ts),
            )
        client = fetch_one("SELECT client_id FROM clients WHERE client_id = ?", (1,))

    member = fetch_one(
        "SELECT member_id, subscriber_id, family_id, client_id FROM members ORDER BY member_id LIMIT 1"
    )

    if not member:
        now_ts = _utcnow_iso()
        with db_session() as conn:
            conn.execute(
                """
                INSERT INTO members (
                    member_id, subscriber_id, client_id,
                    first_name, last_name, dob, gender,
                    relationship_code, family_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SCN_MEMBER_0001", "SCN_SUB_0001", client["client_id"],
                    "Scenario", "Member", "1990-01-01", "F",
                    "SELF", "SCN_FAM_0001", now_ts,
                ),
            )
        member = fetch_one(
            "SELECT member_id, subscriber_id, family_id, client_id FROM members WHERE member_id = ?",
            ("SCN_MEMBER_0001",),
        )

    plan = fetch_one("SELECT plan_id FROM benefit_plans WHERE active_flag = 1 ORDER BY plan_id LIMIT 1")

    if not plan:
        now_ts = _utcnow_iso()
        with db_session() as conn:
            cur = conn.execute(
                """
                INSERT INTO benefit_plans (
                    plan_code, plan_name, plan_type, client_id, benefit_year,
                    individual_deductible, family_deductible,
                    individual_oop_max, family_oop_max,
                    coinsurance_rate, primary_copay, specialist_copay,
                    preventive_exempt_flag, family_accumulation_type,
                    active_flag, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SCN_PLAN_0001", "Scenario Plan", "PPO", client["client_id"],
                    2026, 1000.00, 2000.00, 5000.00, 10000.00,
                    0.20, 20.00, 40.00, 1, "EMBEDDED", 1, now_ts,
                ),
            )
            synthetic_plan_id = cur.lastrowid
        plan = fetch_one("SELECT plan_id FROM benefit_plans WHERE plan_id = ?", (synthetic_plan_id,))

    vendor = fetch_one("SELECT vendor_id FROM vendors ORDER BY vendor_id LIMIT 1")

    return {
        "member_id": member["member_id"],
        "subscriber_id": member["subscriber_id"],
        "family_id": member["family_id"],
        "client_id": member["client_id"],
        "plan_id": plan["plan_id"],
        "vendor_id": vendor["vendor_id"] if vendor else None,
        "coverage_start": None,
        "coverage_end": None,
        "status": None,
        "context_source": "MEMBER_BOOTSTRAP",
    }


def _get_default_member_context():
    eligibility_ctx = _get_member_context_with_eligibility()
    if eligibility_ctx:
        family_row = fetch_one(
            "SELECT family_id FROM members WHERE member_id = ?",
            (eligibility_ctx["member_id"],),
        )
        return {
            "member_id": eligibility_ctx["member_id"],
            "subscriber_id": eligibility_ctx["subscriber_id"],
            "family_id": family_row["family_id"] if family_row else None,
            "client_id": eligibility_ctx["client_id"],
            "plan_id": eligibility_ctx["plan_id"],
            "vendor_id": eligibility_ctx["vendor_id"],
            "coverage_start": eligibility_ctx["coverage_start"],
            "coverage_end": eligibility_ctx["coverage_end"],
            "status": eligibility_ctx["status"],
            "context_source": "ELIGIBILITY_PERIOD",
        }
    return _ensure_member_context()


def _derive_ineligible_service_date(coverage_start, coverage_end):
    if coverage_end:
        end_dt = datetime.fromisoformat(str(coverage_end)).date()
        return (end_dt + timedelta(days=1)).isoformat()
    if coverage_start:
        start_dt = datetime.fromisoformat(str(coverage_start)).date()
        return (start_dt - timedelta(days=1)).isoformat()
    return _today_date()


def _find_existing_open_issue(member_id, service_date):
    return fetch_one(
        """
        SELECT issue_id, claim_record_id, run_id
        FROM data_quality_issues
        WHERE issue_code = 'CLAIM_INELIGIBLE_MEMBER'
          AND member_id = ?
          AND status = 'OPEN'
          AND entity_key = ?
        ORDER BY issue_id DESC
        LIMIT 1
        """,
        (member_id, service_date),
    )


def _insert_claim_record(member_id, subscriber_id, client_id, plan_id, vendor_id, service_date):
    now_ts = _utcnow_iso()
    claim_id = _generate_claim_id(member_id, service_date)
    line_id = _generate_line_id(member_id, service_date)

    with db_session() as conn:
        cur = conn.execute(
            """
            INSERT INTO claims (
                claim_id, line_id, member_id, subscriber_id, client_id,
                plan_id, vendor_id, service_date, paid_date,
                allowed_amount, paid_amount, member_responsibility,
                deductible_amount, coinsurance_amount, copay_amount,
                preventive_flag, reversal_flag, claim_status,
                source_file_id, source_row_number, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim_id, line_id, member_id, subscriber_id, client_id,
                plan_id, vendor_id, service_date, None,
                250.00, 0.00, 250.00, 0.00, 0.00, 0.00,
                0, 0, "DENIED", None, None, now_ts,
            ),
        )
        return cur.lastrowid


def _insert_claim_processing_run(claim_record_id):
    started_at = _utcnow_iso()
    completed_at = _utcnow_iso()

    with db_session() as conn:
        cur = conn.execute(
            """
            INSERT INTO processing_runs (
                run_type, file_id, started_at, completed_at, run_status,
                rows_read, rows_passed, rows_failed, issue_count, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                DEFAULT_RUN_TYPE, None, started_at, completed_at, "COMPLETED",
                1, 0, 1, 1,
                f"Scenario loader created claim processing run for ineligible member claim_record_id={claim_record_id}.",
            ),
        )
        return cur.lastrowid


def _insert_ineligible_claim_issue(
    client_id, vendor_id, claim_record_id, run_id, member_id,
    service_date, context_source, coverage_start, coverage_end,
):
    detected_at = _utcnow_iso()

    if coverage_start or coverage_end:
        description = (
            f"Claim was received for member_id={member_id} with service_date={service_date}, "
            f"but no active eligibility period covered the service date. "
            f"Known coverage window: start={coverage_start}, end={coverage_end}."
        )
    else:
        description = (
            f"Claim was received for member_id={member_id} with service_date={service_date}, "
            f"but no active eligibility period was available to support claim adjudication."
        )

    message = (
        f"Claim for ineligible member. member_id={member_id}, "
        f"claim_record_id={claim_record_id}, service_date={service_date}, "
        f"context_source={context_source}."
    )

    with db_session() as conn:
        cur = conn.execute(
            """
            INSERT INTO data_quality_issues (
                issue_type, issue_subtype, issue_code, issue_message,
                severity, status, client_id, vendor_id, file_id, run_id,
                member_id, claim_record_id, entity_name, entity_key,
                source_row_number, issue_description,
                detected_at, resolved_at, resolution_notes,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "CLAIMS", "ELIGIBILITY_MISMATCH",
                "CLAIM_INELIGIBLE_MEMBER", message,
                DEFAULT_SEVERITY, "OPEN",
                client_id, vendor_id, None, run_id,
                member_id, claim_record_id, "claims", service_date,
                None, description,
                detected_at, None, None,
                detected_at, detected_at,
            ),
        )
        return cur.lastrowid


def _fetch_scenario_summary(issue_id: int):
    return fetch_one(
        """
        SELECT
            dqi.issue_id, dqi.issue_code, dqi.severity,
            sc.case_id, sc.priority, sc.assigned_team,
            st.sla_id, st.target_hours
        FROM data_quality_issues dqi
        LEFT JOIN support_cases sc ON sc.issue_id = dqi.issue_id
        LEFT JOIN sla_tracking st ON st.case_id = sc.case_id
        WHERE dqi.issue_id = ?
        ORDER BY st.sla_id DESC
        LIMIT 1
        """,
        (issue_id,),
    )


def _insert_audit_log(event_type, entity_name, entity_id, message):
    now_ts = _utcnow_iso()
    try:
        execute(
            """
            INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event_type, entity_name, str(entity_id), "system", message),
        )
    except Exception:
        pass


def run(service_date=None):
    member_ctx = _get_default_member_context()

    member_id = member_ctx["member_id"]
    subscriber_id = member_ctx["subscriber_id"]
    family_id = member_ctx.get("family_id")
    client_id = member_ctx["client_id"]
    plan_id = member_ctx["plan_id"]
    vendor_id = member_ctx["vendor_id"]
    coverage_start = member_ctx["coverage_start"]
    coverage_end = member_ctx["coverage_end"]
    eligibility_status = member_ctx["status"]
    context_source = member_ctx["context_source"]

    service_date = service_date or _derive_ineligible_service_date(coverage_start, coverage_end)

    existing_issue = _find_existing_open_issue(member_id, service_date)
    if existing_issue:
        print("Scenario already active. Existing open ineligible-claim issue found.")
        result = {
            "issue_id": existing_issue["issue_id"],
            "claim_record_id": existing_issue["claim_record_id"],
            "run_id": existing_issue["run_id"],
            "member_id": member_id,
            "service_date": service_date,
        }
        print(result)
        try:
            run_case_generation_main()
        except TypeError:
            run_case_generation_main()
        return result

    claim_record_id = _insert_claim_record(member_id, subscriber_id, client_id, plan_id, vendor_id, service_date)
    run_id = _insert_claim_processing_run(claim_record_id)

    issue_id = _insert_ineligible_claim_issue(
        client_id, vendor_id, claim_record_id, run_id, member_id,
        service_date, context_source, coverage_start, coverage_end,
    )

    _insert_audit_log("SCENARIO_LOADED", "data_quality_issues", issue_id,
                       f"Loaded deterministic scenario: {SCENARIO_NAME}")

    try:
        run_case_generation_main()
    except TypeError:
        run_case_generation_main()

    created_case = fetch_one(
        "SELECT case_id, priority, status, assigned_team FROM support_cases WHERE issue_id = ? ORDER BY case_id DESC LIMIT 1",
        (issue_id,),
    )

    if created_case:
        try:
            create_sla_for_case(created_case["case_id"])
        except Exception:
            pass

    try:
        evaluate_open_slas()
    except Exception:
        pass

    created_sla = None
    if created_case:
        created_sla = fetch_one(
            "SELECT sla_id, sla_type, target_hours, target_due_at, status, is_at_risk, is_breached FROM sla_tracking WHERE case_id = ? ORDER BY sla_id DESC LIMIT 1",
            (created_case["case_id"],),
        )

    summary = {
        "scenario_name": SCENARIO_NAME,
        "context_source": context_source,
        "member_id": member_id,
        "subscriber_id": subscriber_id,
        "family_id": family_id,
        "client_id": client_id,
        "plan_id": plan_id,
        "vendor_id": vendor_id,
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
        "eligibility_status": eligibility_status,
        "service_date": service_date,
        "claim_record_id": claim_record_id,
        "run_id": run_id,
        "issue_id": issue_id,
        "case": dict(created_case) if created_case else None,
        "sla": dict(created_sla) if created_sla else None,
    }

    print("Scenario loaded successfully:")
    print(summary)

    scenario_summary = _fetch_scenario_summary(issue_id)
    if scenario_summary:
        print(f"\nScenario Summary:")
        print(f"Issue Severity: {scenario_summary['severity']}")
        print(f"Case Priority: {scenario_summary['priority']}")
        print(f"Assigned Team: {scenario_summary['assigned_team']}")
        print(f"SLA Hours: {scenario_summary['target_hours']}")

    return summary


if __name__ == "__main__":
    run()