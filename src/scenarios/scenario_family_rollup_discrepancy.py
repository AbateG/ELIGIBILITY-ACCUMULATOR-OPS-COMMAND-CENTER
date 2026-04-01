from __future__ import annotations

from datetime import UTC, datetime

from src.common.datetime_utils import utc_now_iso
from src.common.db import get_connection
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.sla.sla_service import create_sla_for_case, evaluate_open_slas


SCENARIO_ISSUE_CODE = "FAMILY_ROLLUP_DISCREPANCY"
SCENARIO_RUN_TYPE = "SCENARIO_LOAD"
SCENARIO_FILE_NAME = "scenario_family_rollup_discrepancy.csv"
SCENARIO_SERVICE_DATE = "2024-07-15"
SCENARIO_BENEFIT_YEAR = 2024

SCENARIO_SUBSCRIBER_MEMBER_ID = "SCN_FAM_SUB_0001"
SCENARIO_DEPENDENT_MEMBER_ID = "SCN_FAM_DEP_0001"
SCENARIO_SUBSCRIBER_ID = "SCN_FAM_SUB_0001"
SCENARIO_FAMILY_ID = "SCN_FAMILY_0001"

SCENARIO_SUB_CLAIM_ID = "SCN-FAM-ROLLUP-SUB-0001"
SCENARIO_DEP_CLAIM_ID = "SCN-FAM-ROLLUP-DEP-0001"
SCENARIO_LINE_ID = "1"

SCENARIO_ENTITY_NAME = "accumulator_snapshots"
SCENARIO_ENTITY_KEY = f"{SCENARIO_FAMILY_ID}:{SCENARIO_BENEFIT_YEAR}:FAMILY_OOP"


def get_or_create_client_id(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM clients ORDER BY client_id LIMIT 1")
    row = cur.fetchone()
    if row:
        return row["client_id"]
    cur.execute(
        "INSERT INTO clients (client_code, client_name, active_flag, created_at) VALUES (?, ?, ?, ?)",
        ("SCN_CLIENT", "Scenario Client", 1, utc_now_iso()),
    )
    conn.commit()
    return cur.lastrowid


def get_first_active_plan(conn, client_id) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT plan_id, plan_code, plan_name, plan_type, benefit_year,
               individual_deductible, family_deductible,
               individual_oop_max, family_oop_max, family_accumulation_type, active_flag
        FROM benefit_plans WHERE active_flag = 1
        ORDER BY plan_id LIMIT 1
        """
    )
    row = cur.fetchone()
    if row:
        return dict(row)

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
    conn.commit()
    cur.execute(
        """
        SELECT plan_id, plan_code, plan_name, plan_type, benefit_year,
               individual_deductible, family_deductible,
               individual_oop_max, family_oop_max, family_accumulation_type, active_flag
        FROM benefit_plans WHERE plan_id = ?
        """,
        (plan_id,),
    )
    return dict(cur.fetchone())


def get_first_vendor_id(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT vendor_id FROM vendors ORDER BY vendor_id LIMIT 1")
    row = cur.fetchone()
    if row:
        return row["vendor_id"]
    cur.execute(
        "INSERT INTO vendors (vendor_code, vendor_name, vendor_type, active_flag, created_at) VALUES (?, ?, ?, ?, ?)",
        ("SCN_VENDOR", "Scenario Vendor", "TPA", 1, utc_now_iso()),
    )
    conn.commit()
    return cur.lastrowid


def ensure_family_members(conn, client_id: int) -> dict:
    cur = conn.cursor()

    cur.execute("SELECT member_id FROM members WHERE member_id = ?", (SCENARIO_SUBSCRIBER_MEMBER_ID,))
    if not cur.fetchone():
        cur.execute(
            """
            INSERT INTO members (
                member_id, subscriber_id, client_id,
                first_name, last_name, dob, gender,
                relationship_code, family_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (SCENARIO_SUBSCRIBER_MEMBER_ID, SCENARIO_SUBSCRIBER_ID, client_id,
             "Scenario", "Subscriber", "1982-03-10", "U",
             "18", SCENARIO_FAMILY_ID, utc_now_iso()),
        )

    cur.execute("SELECT member_id FROM members WHERE member_id = ?", (SCENARIO_DEPENDENT_MEMBER_ID,))
    if not cur.fetchone():
        cur.execute(
            """
            INSERT INTO members (
                member_id, subscriber_id, client_id,
                first_name, last_name, dob, gender,
                relationship_code, family_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (SCENARIO_DEPENDENT_MEMBER_ID, SCENARIO_SUBSCRIBER_ID, client_id,
             "Scenario", "Dependent", "2010-08-21", "U",
             "19", SCENARIO_FAMILY_ID, utc_now_iso()),
        )

    conn.commit()
    return {
        "subscriber_member_id": SCENARIO_SUBSCRIBER_MEMBER_ID,
        "dependent_member_id": SCENARIO_DEPENDENT_MEMBER_ID,
        "subscriber_id": SCENARIO_SUBSCRIBER_ID,
        "family_id": SCENARIO_FAMILY_ID,
        "client_id": client_id,
    }


def ensure_eligibility_period(conn, member_id, subscriber_id, client_id, plan_id, vendor_id) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT eligibility_id FROM eligibility_periods
        WHERE member_id = ? AND client_id = ? AND plan_id = ? AND vendor_id = ?
          AND coverage_start <= ? AND coverage_end >= ?
        ORDER BY eligibility_id DESC LIMIT 1
        """,
        (member_id, client_id, plan_id, vendor_id, SCENARIO_SERVICE_DATE, SCENARIO_SERVICE_DATE),
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
        (member_id, subscriber_id, client_id, plan_id, vendor_id,
         "SCN_FAM_GRP_001", "2024-01-01", "2024-12-31", "ACTIVE",
         None, 1, utc_now_iso()),
    )
    conn.commit()
    return cur.lastrowid


def ensure_scenario_file(conn, client_id, vendor_id) -> int:
    cur = conn.cursor()
    cur.execute("SELECT file_id FROM inbound_files WHERE file_name = ? ORDER BY file_id DESC LIMIT 1",
                (SCENARIO_FILE_NAME,))
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
        (SCENARIO_FILE_NAME, "CLAIMS", client_id, vendor_id,
         SCENARIO_SERVICE_DATE, now_str,
         "scenario-family-rollup-discrepancy-hash", 2, "RECEIVED",
         0, 0, f"landing/{SCENARIO_FILE_NAME}",
         f"archive/{SCENARIO_FILE_NAME}", now_str),
    )
    conn.commit()
    return cur.lastrowid


def ensure_processing_run(conn, file_id) -> int:
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
        (SCENARIO_RUN_TYPE, file_id, now_str, now_str, "COMPLETED",
         2, 2, 0, 1,
         "Deterministic scenario load for family rollup discrepancy."),
    )
    conn.commit()
    return cur.lastrowid


def ensure_claim_record(conn, claim_id, member_id, subscriber_id, client_id,
                         plan_id, vendor_id, file_id, member_responsibility) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT claim_record_id FROM claims WHERE claim_id = ? AND line_id = ? ORDER BY claim_record_id DESC LIMIT 1",
        (claim_id, SCENARIO_LINE_ID),
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
            claim_id, SCENARIO_LINE_ID, member_id, subscriber_id, client_id,
            plan_id, vendor_id, SCENARIO_SERVICE_DATE, SCENARIO_SERVICE_DATE,
            member_responsibility + 300.00, member_responsibility + 100.00,
            member_responsibility, 0.00, member_responsibility, 0.00,
            0, 0, "PAID", file_id, 1, utc_now_iso(),
        ),
    )
    conn.commit()
    return cur.lastrowid


def ensure_individual_oop_transaction(conn, member_id, family_id, client_id,
                                       plan_id, claim_record_id, file_id, delta_amount) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT accumulator_txn_id FROM accumulator_transactions
        WHERE member_id = ? AND claim_record_id = ? AND benefit_year = ?
          AND accumulator_type = 'IND_OOP'
        ORDER BY accumulator_txn_id DESC LIMIT 1
        """,
        (member_id, claim_record_id, SCENARIO_BENEFIT_YEAR),
    )
    row = cur.fetchone()
    if row:
        return row["accumulator_txn_id"]

    cur.execute(
        """
        INSERT INTO accumulator_transactions (
            member_id, family_id, client_id, plan_id, claim_record_id,
            benefit_year, accumulator_type, delta_amount, service_date,
            source_type, source_file_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (member_id, family_id, client_id, plan_id, claim_record_id,
         SCENARIO_BENEFIT_YEAR, "IND_OOP", delta_amount,
         SCENARIO_SERVICE_DATE, "CLAIM", file_id, utc_now_iso()),
    )
    conn.commit()
    return cur.lastrowid


def ensure_family_discrepant_snapshot(conn, family_id, subscriber_member_id,
                                       client_id, plan_id,
                                       expected_family_oop, discrepant_family_oop) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT snapshot_id FROM accumulator_snapshots
        WHERE member_id = ? AND family_id = ? AND benefit_year = ? AND family_oop_accum = ?
        ORDER BY snapshot_id DESC LIMIT 1
        """,
        (subscriber_member_id, family_id, SCENARIO_BENEFIT_YEAR, discrepant_family_oop),
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
        (subscriber_member_id, family_id, client_id, plan_id,
         SCENARIO_BENEFIT_YEAR, 0.00, 0.00,
         expected_family_oop, discrepant_family_oop,
         0, 0, 0, 0, utc_now_iso()),
    )
    conn.commit()
    return cur.lastrowid


def fetch_expected_family_oop(conn, family_id, plan_id) -> float:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(delta_amount), 0.0) AS expected_family_oop
        FROM accumulator_transactions
        WHERE family_id = ? AND plan_id = ? AND benefit_year = ?
          AND accumulator_type = 'IND_OOP'
        """,
        (family_id, plan_id, SCENARIO_BENEFIT_YEAR),
    )
    return float(cur.fetchone()["expected_family_oop"])


def fetch_latest_family_snapshot(conn, family_id, subscriber_member_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT snapshot_id, member_id, family_id, client_id, plan_id,
               benefit_year, individual_oop_accum, family_oop_accum, snapshot_ts
        FROM accumulator_snapshots
        WHERE family_id = ? AND member_id = ? AND benefit_year = ?
        ORDER BY snapshot_id DESC LIMIT 1
        """,
        (family_id, subscriber_member_id, SCENARIO_BENEFIT_YEAR),
    )
    return cur.fetchone()


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


def create_issue_if_needed(conn, client_id, vendor_id, file_id, run_id,
                            subscriber_member_id, expected_family_oop,
                            actual_family_oop, claim_record_id) -> int:
    existing = find_existing_open_issue(conn)
    if existing:
        return existing["issue_id"]

    now_str = utc_now_iso()
    discrepancy_amount = expected_family_oop - actual_family_oop

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
            "ACCUMULATORS", "FAMILY_ROLLUP",
            SCENARIO_ISSUE_CODE,
            (
                f"Family accumulator rollup mismatch for family_id {SCENARIO_FAMILY_ID}: "
                f"expected {expected_family_oop:.2f}, actual {actual_family_oop:.2f}."
            ),
            "HIGH", "OPEN",
            client_id, vendor_id, file_id, run_id,
            subscriber_member_id, claim_record_id,
            SCENARIO_ENTITY_NAME, SCENARIO_ENTITY_KEY,
            1,
            (
                f"Family rollup discrepancy detected for family_id {SCENARIO_FAMILY_ID} "
                f"benefit year {SCENARIO_BENEFIT_YEAR}. "
                f"Expected family OOP = {expected_family_oop:.2f}; "
                f"actual family OOP snapshot = {actual_family_oop:.2f}; "
                f"discrepancy = {discrepancy_amount:.2f}."
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


def main():
    conn = get_connection()
    try:
        client_id = get_or_create_client_id(conn)
        plan = get_first_active_plan(conn, client_id)
        vendor_id = get_first_vendor_id(conn)
        family = ensure_family_members(conn, client_id)

        ensure_eligibility_period(conn, family["subscriber_member_id"],
                                   family["subscriber_id"], client_id,
                                   plan["plan_id"], vendor_id)
        ensure_eligibility_period(conn, family["dependent_member_id"],
                                   family["subscriber_id"], client_id,
                                   plan["plan_id"], vendor_id)

        file_id = ensure_scenario_file(conn, client_id, vendor_id)
        run_id = ensure_processing_run(conn, file_id)

        sub_claim_record_id = ensure_claim_record(
            conn, SCENARIO_SUB_CLAIM_ID, family["subscriber_member_id"],
            family["subscriber_id"], client_id, plan["plan_id"],
            vendor_id, file_id, 800.00,
        )
        dep_claim_record_id = ensure_claim_record(
            conn, SCENARIO_DEP_CLAIM_ID, family["dependent_member_id"],
            family["subscriber_id"], client_id, plan["plan_id"],
            vendor_id, file_id, 700.00,
        )

        ensure_individual_oop_transaction(conn, family["subscriber_member_id"],
                                           family["family_id"], client_id,
                                           plan["plan_id"], sub_claim_record_id,
                                           file_id, 800.00)
        ensure_individual_oop_transaction(conn, family["dependent_member_id"],
                                           family["family_id"], client_id,
                                           plan["plan_id"], dep_claim_record_id,
                                           file_id, 700.00)

        expected_family_oop = fetch_expected_family_oop(conn, family["family_id"], plan["plan_id"])
        discrepant_family_oop = expected_family_oop - 300.00

        snapshot_id = ensure_family_discrepant_snapshot(
            conn, family["family_id"], family["subscriber_member_id"],
            client_id, plan["plan_id"],
            expected_family_oop, discrepant_family_oop,
        )

        latest_snapshot = fetch_latest_family_snapshot(conn, family["family_id"],
                                                        family["subscriber_member_id"])
        actual_family_oop = float(latest_snapshot["family_oop_accum"])

        if expected_family_oop == actual_family_oop:
            raise RuntimeError(
                "Scenario setup failed: expected family OOP equals actual family OOP; "
                "discrepancy was not created."
            )

        issue_id = create_issue_if_needed(
            conn, client_id, vendor_id, file_id, run_id,
            family["subscriber_member_id"], expected_family_oop,
            actual_family_oop, sub_claim_record_id,
        )

        case_id, sla_id = ensure_case_and_sla(conn, issue_id)

        print("Scenario loaded successfully:")
        print(f"  issue_code            = {SCENARIO_ISSUE_CODE}")
        print(f"  family_id             = {family['family_id']}")
        print(f"  subscriber_member_id  = {family['subscriber_member_id']}")
        print(f"  dependent_member_id   = {family['dependent_member_id']}")
        print(f"  plan_id               = {plan['plan_id']}")
        print(f"  snapshot_id           = {snapshot_id}")
        print(f"  expected_family_oop   = {expected_family_oop:.2f}")
        print(f"  actual_family_oop     = {actual_family_oop:.2f}")
        print(f"  issue_id              = {issue_id}")
        print(f"  case_id               = {case_id}")
        print(f"  sla_id                = {sla_id}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()