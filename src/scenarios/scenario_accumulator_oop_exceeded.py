from __future__ import annotations

from datetime import UTC, datetime

from src.common.datetime_utils import utc_now_iso
from src.common.db import get_connection
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.sla.sla_service import create_sla_for_case, evaluate_open_slas


SCENARIO_ISSUE_CODE = "ACCUMULATOR_EXCEEDS_OOP_MAX"
SCENARIO_MEMBER_ID = "SCN_ACC_MEMBER_0001"
SCENARIO_SUBSCRIBER_ID = "SCN_ACC_SUB_0001"
SCENARIO_FAMILY_ID = "SCN_ACC_FAM_0001"
SCENARIO_CLAIM_ID = "SCN-ACC-OOP-0001"
SCENARIO_LINE_ID = "1"
SCENARIO_FILE_NAME = "scenario_accumulator_oop_exceeded.csv"
SCENARIO_RUN_TYPE = "SCENARIO_LOAD"
SCENARIO_SERVICE_DATE = "2024-06-15"
SCENARIO_BENEFIT_YEAR = 2024
SCENARIO_ENTITY_NAME = "accumulator_snapshots"
SCENARIO_ENTITY_KEY = f"{SCENARIO_MEMBER_ID}:{SCENARIO_BENEFIT_YEAR}:IND_OOP"


def get_or_create_scenario_member(conn) -> dict:
    cur = conn.cursor()
    cur.execute(
        "SELECT member_id, subscriber_id, client_id, family_id FROM members WHERE member_id = ?",
        (SCENARIO_MEMBER_ID,),
    )
    row = cur.fetchone()
    if row:
        return dict(row)

    cur.execute("SELECT client_id FROM clients ORDER BY client_id LIMIT 1")
    client_row = cur.fetchone()

    if not client_row:
        cur.execute(
            "INSERT INTO clients (client_code, client_name, active_flag, created_at) VALUES (?, ?, ?, ?)",
            ("SCN_CLIENT", "Scenario Client", 1, utc_now_iso()),
        )
        client_id = cur.lastrowid
    else:
        client_id = client_row["client_id"]

    now_str = utc_now_iso()
    cur.execute(
        """
        INSERT INTO members (
            member_id, subscriber_id, client_id,
            first_name, last_name, dob, gender,
            relationship_code, family_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            SCENARIO_MEMBER_ID, SCENARIO_SUBSCRIBER_ID, client_id,
            "Scenario", "Accumulator", "1985-05-20", "U",
            "18", SCENARIO_FAMILY_ID, now_str,
        ),
    )
    conn.commit()

    return {
        "member_id": SCENARIO_MEMBER_ID,
        "subscriber_id": SCENARIO_SUBSCRIBER_ID,
        "client_id": client_id,
        "family_id": SCENARIO_FAMILY_ID,
    }


def get_first_active_plan(conn, client_id) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT plan_id, plan_code, plan_name, plan_type, benefit_year,
               individual_deductible, family_deductible,
               individual_oop_max, family_oop_max
        FROM benefit_plans
        WHERE active_flag = 1
        ORDER BY plan_id
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        cur.execute(
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
                "SCN_PLAN_0001", "Scenario Plan", "PPO", client_id,
                2026, 1000.00, 2000.00, 5000.00, 10000.00,
                0.20, 20.00, 40.00, 1, "EMBEDDED", 1, utc_now_iso(),
            ),
        )
        plan_id = cur.lastrowid
        cur.execute(
            """
            SELECT plan_id, plan_code, plan_name, plan_type, benefit_year,
                   individual_deductible, family_deductible,
                   individual_oop_max, family_oop_max
            FROM benefit_plans WHERE plan_id = ?
            """,
            (plan_id,),
        )
        row = cur.fetchone()
    return dict(row)


def get_first_vendor_id(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT vendor_id FROM vendors ORDER BY vendor_id LIMIT 1")
    row = cur.fetchone()
    if not row:
        cur.execute(
            "INSERT INTO vendors (vendor_code, vendor_name, vendor_type, active_flag, created_at) VALUES (?, ?, ?, ?, ?)",
            ("SCN_VENDOR", "Scenario Vendor", "TPA", 1, utc_now_iso()),
        )
        return cur.lastrowid
    return row["vendor_id"]


def ensure_eligibility_period(conn, member: dict, plan_id: int, vendor_id: int) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT eligibility_id FROM eligibility_periods
        WHERE member_id = ? AND client_id = ? AND plan_id = ? AND vendor_id = ?
          AND coverage_start <= ? AND coverage_end >= ?
        ORDER BY eligibility_id DESC LIMIT 1
        """,
        (member["member_id"], member["client_id"], plan_id, vendor_id,
         SCENARIO_SERVICE_DATE, SCENARIO_SERVICE_DATE),
    )
    row = cur.fetchone()
    if row:
        return row["eligibility_id"]

    cur.execute(
        """
        INSERT INTO eligibility_periods (
            member_id, subscriber_id, client_id, plan_id, vendor_id,
            group_id, coverage_start, coverage_end, status,
            source_file_id, source_row_number, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            member["member_id"], member["subscriber_id"], member["client_id"],
            plan_id, vendor_id, "SCN_ACC_GRP_001",
            "2024-01-01", "2024-12-31", "ACTIVE", None, 1, utc_now_iso(),
        ),
    )
    conn.commit()
    return cur.lastrowid


def ensure_scenario_file(conn, client_id: int, vendor_id: int) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT file_id FROM inbound_files WHERE file_name = ? ORDER BY file_id DESC LIMIT 1",
        (SCENARIO_FILE_NAME,),
    )
    row = cur.fetchone()
    if row:
        return row["file_id"]

    now_str = utc_now_iso()
    cur.execute(
        """
        INSERT INTO inbound_files (
            file_name, file_type, client_id, vendor_id, expected_date,
            received_ts, file_hash, row_count, processing_status,
            duplicate_flag, error_count, landing_path, archived_path, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            SCENARIO_FILE_NAME, "CLAIMS", client_id, vendor_id,
            SCENARIO_SERVICE_DATE, now_str,
            "scenario-accumulator-oop-exceeded-hash", 1, "RECEIVED",
            0, 0, f"landing/{SCENARIO_FILE_NAME}",
            f"archive/{SCENARIO_FILE_NAME}", now_str,
        ),
    )
    conn.commit()
    return cur.lastrowid


def ensure_processing_run(conn, file_id: int) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT run_id FROM processing_runs WHERE file_id = ? AND run_type = ? ORDER BY run_id DESC LIMIT 1",
        (file_id, SCENARIO_RUN_TYPE),
    )
    row = cur.fetchone()
    if row:
        return row["run_id"]

    now_str = utc_now_iso()
    cur.execute(
        """
        INSERT INTO processing_runs (
            run_type, file_id, started_at, completed_at, run_status,
            rows_read, rows_passed, rows_failed, issue_count, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            SCENARIO_RUN_TYPE, file_id, now_str, now_str, "COMPLETED",
            1, 1, 0, 1,
            "Deterministic scenario load for accumulator OOP max exceedance.",
        ),
    )
    conn.commit()
    return cur.lastrowid


def ensure_claim_record(conn, member: dict, plan_id: int, vendor_id: int, file_id: int) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT claim_record_id FROM claims WHERE claim_id = ? AND line_id = ? ORDER BY claim_record_id DESC LIMIT 1",
        (SCENARIO_CLAIM_ID, SCENARIO_LINE_ID),
    )
    row = cur.fetchone()
    if row:
        return row["claim_record_id"]

    cur.execute(
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
            SCENARIO_CLAIM_ID, SCENARIO_LINE_ID,
            member["member_id"], member["subscriber_id"], member["client_id"],
            plan_id, vendor_id, SCENARIO_SERVICE_DATE, SCENARIO_SERVICE_DATE,
            500.00, 350.00, 150.00, 0.00, 150.00, 0.00,
            0, 0, "PAID", file_id, 1, utc_now_iso(),
        ),
    )
    conn.commit()
    return cur.lastrowid


def ensure_accumulator_transactions(conn, member, plan_id, claim_record_id, file_id,
                                     pre_breach_oop, oop_delta):
    cur = conn.cursor()

    # Baseline
    cur.execute(
        """
        SELECT accumulator_txn_id FROM accumulator_transactions
        WHERE member_id = ? AND claim_record_id IS NULL AND benefit_year = ?
          AND accumulator_type = 'IND_OOP' AND source_type = 'SCENARIO_BASELINE'
        ORDER BY accumulator_txn_id DESC LIMIT 1
        """,
        (member["member_id"], SCENARIO_BENEFIT_YEAR),
    )
    if not cur.fetchone():
        cur.execute(
            """
            INSERT INTO accumulator_transactions (
                member_id, family_id, client_id, plan_id, claim_record_id,
                benefit_year, accumulator_type, delta_amount, service_date,
                source_type, source_file_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                member["member_id"], member["family_id"], member["client_id"],
                plan_id, None, SCENARIO_BENEFIT_YEAR, "IND_OOP",
                pre_breach_oop, "2024-01-01", "SCENARIO_BASELINE",
                file_id, utc_now_iso(),
            ),
        )

    # Breach-causing
    cur.execute(
        """
        SELECT accumulator_txn_id FROM accumulator_transactions
        WHERE member_id = ? AND claim_record_id = ? AND benefit_year = ?
          AND accumulator_type = 'IND_OOP'
        ORDER BY accumulator_txn_id DESC LIMIT 1
        """,
        (member["member_id"], claim_record_id, SCENARIO_BENEFIT_YEAR),
    )
    if not cur.fetchone():
        cur.execute(
            """
            INSERT INTO accumulator_transactions (
                member_id, family_id, client_id, plan_id, claim_record_id,
                benefit_year, accumulator_type, delta_amount, service_date,
                source_type, source_file_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                member["member_id"], member["family_id"], member["client_id"],
                plan_id, claim_record_id, SCENARIO_BENEFIT_YEAR,
                "IND_OOP", oop_delta, SCENARIO_SERVICE_DATE,
                "CLAIM", file_id, utc_now_iso(),
            ),
        )

    conn.commit()


def ensure_breached_snapshot(conn, member, plan, pre_breach_oop, oop_delta) -> int:
    cur = conn.cursor()
    breached_oop = pre_breach_oop + oop_delta
    individual_oop_met_flag = 1 if breached_oop >= plan["individual_oop_max"] else 0

    cur.execute(
        """
        SELECT snapshot_id FROM accumulator_snapshots
        WHERE member_id = ? AND benefit_year = ? AND individual_oop_accum = ?
        ORDER BY snapshot_id DESC LIMIT 1
        """,
        (member["member_id"], SCENARIO_BENEFIT_YEAR, breached_oop),
    )
    row = cur.fetchone()
    if row:
        return row["snapshot_id"]

    cur.execute(
        """
        INSERT INTO accumulator_snapshots (
            member_id, family_id, client_id, plan_id, benefit_year,
            individual_deductible_accum, family_deductible_accum,
            individual_oop_accum, family_oop_accum,
            individual_deductible_met_flag, family_deductible_met_flag,
            individual_oop_met_flag, family_oop_met_flag, snapshot_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            member["member_id"], member["family_id"], member["client_id"],
            plan["plan_id"], SCENARIO_BENEFIT_YEAR,
            0.00, 0.00, breached_oop, breached_oop,
            0, 0, individual_oop_met_flag, 0, utc_now_iso(),
        ),
    )
    conn.commit()
    return cur.lastrowid


def find_existing_open_issue(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT issue_id FROM data_quality_issues
        WHERE issue_code = ? AND status = 'OPEN' AND entity_name = ? AND entity_key = ?
        ORDER BY issue_id DESC LIMIT 1
        """,
        (SCENARIO_ISSUE_CODE, SCENARIO_ENTITY_NAME, SCENARIO_ENTITY_KEY),
    )
    return cur.fetchone()


def create_issue_if_needed(conn, member, vendor_id, file_id, run_id,
                            claim_record_id, plan, breached_oop) -> int:
    existing = find_existing_open_issue(conn)
    if existing:
        return existing["issue_id"]

    now_str = utc_now_iso()
    cur = conn.cursor()
    cur.execute(
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
            "ACCUMULATORS", "OOP_MAX",
            SCENARIO_ISSUE_CODE,
            (
                f"Accumulator exceeds OOP max for member {member['member_id']} "
                f"in benefit year {SCENARIO_BENEFIT_YEAR}: "
                f"{breached_oop:.2f} > {plan['individual_oop_max']:.2f}."
            ),
            "HIGH", "OPEN",
            member["client_id"], vendor_id, file_id, run_id,
            member["member_id"], claim_record_id,
            SCENARIO_ENTITY_NAME, SCENARIO_ENTITY_KEY,
            1,
            (
                f"Member {member['member_id']} individual out-of-pocket accumulator "
                f"for benefit year {SCENARIO_BENEFIT_YEAR} exceeded plan maximum. "
                f"Plan OOP max = {plan['individual_oop_max']:.2f}; "
                f"actual accumulator = {breached_oop:.2f}."
            ),
            now_str, None, None,
            now_str, now_str,
        ),
    )
    conn.commit()
    return cur.lastrowid


def ensure_case_and_sla(conn, issue_id: int) -> tuple[int, int | None]:
    cur = conn.cursor()

    cur.execute("SELECT case_id FROM support_cases WHERE issue_id = ? ORDER BY case_id DESC LIMIT 1", (issue_id,))
    case_row = cur.fetchone()

    if not case_row:
        create_support_cases_from_open_issues()
        cur.execute("SELECT case_id FROM support_cases WHERE issue_id = ? ORDER BY case_id DESC LIMIT 1", (issue_id,))
        case_row = cur.fetchone()

    if not case_row:
        raise RuntimeError(f"Support case was not created for issue_id={issue_id}")

    case_id = case_row["case_id"]

    cur.execute("SELECT sla_id FROM sla_tracking WHERE case_id = ? ORDER BY sla_id DESC LIMIT 1", (case_id,))
    sla_row = cur.fetchone()

    if not sla_row:
        create_sla_for_case(case_id)
        cur.execute("SELECT sla_id FROM sla_tracking WHERE case_id = ? ORDER BY sla_id DESC LIMIT 1", (case_id,))
        sla_row = cur.fetchone()

    evaluate_open_slas()

    return case_id, sla_row["sla_id"] if sla_row else None


def fetch_current_total_individual_oop(conn, member_id, plan_id) -> float:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(delta_amount), 0.0) AS total_oop
        FROM accumulator_transactions
        WHERE member_id = ? AND plan_id = ? AND benefit_year = ?
          AND accumulator_type = 'IND_OOP'
        """,
        (member_id, plan_id, SCENARIO_BENEFIT_YEAR),
    )
    row = cur.fetchone()
    return float(row["total_oop"])


def main():
    conn = get_connection()
    try:
        member = get_or_create_scenario_member(conn)
        plan = get_first_active_plan(conn, member["client_id"])
        vendor_id = get_first_vendor_id(conn)

        ensure_eligibility_period(conn, member, plan["plan_id"], vendor_id)
        file_id = ensure_scenario_file(conn, member["client_id"], vendor_id)
        run_id = ensure_processing_run(conn, file_id)
        claim_record_id = ensure_claim_record(conn, member, plan["plan_id"], vendor_id, file_id)

        individual_oop_max = float(plan["individual_oop_max"])
        if individual_oop_max <= 0:
            raise RuntimeError(f"Selected plan_id={plan['plan_id']} has invalid individual_oop_max={individual_oop_max}.")

        pre_breach_oop = max(individual_oop_max - 100.0, 0.0)
        oop_delta = 150.0

        ensure_accumulator_transactions(conn, member, plan["plan_id"], claim_record_id, file_id,
                                         pre_breach_oop, oop_delta)
        snapshot_id = ensure_breached_snapshot(conn, member, plan, pre_breach_oop, oop_delta)
        breached_oop = fetch_current_total_individual_oop(conn, member["member_id"], plan["plan_id"])

        issue_id = create_issue_if_needed(conn, member, vendor_id, file_id, run_id,
                                           claim_record_id, plan, breached_oop)
        case_id, sla_id = ensure_case_and_sla(conn, issue_id)

        print("Scenario loaded successfully:")
        print(f"  issue_code          = {SCENARIO_ISSUE_CODE}")
        print(f"  member_id           = {member['member_id']}")
        print(f"  plan_id             = {plan['plan_id']}")
        print(f"  claim_id            = {SCENARIO_CLAIM_ID}")
        print(f"  claim_record_id     = {claim_record_id}")
        print(f"  snapshot_id         = {snapshot_id}")
        print(f"  individual_oop_max  = {individual_oop_max:.2f}")
        print(f"  actual_oop_accum    = {breached_oop:.2f}")
        print(f"  issue_id            = {issue_id}")
        print(f"  case_id             = {case_id}")
        print(f"  sla_id              = {sla_id}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()