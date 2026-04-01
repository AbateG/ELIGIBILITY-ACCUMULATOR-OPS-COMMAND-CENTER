from src.common.db import fetch_all, fetch_one
from src.scenarios.scenario_duplicate_eligibility_resend import run


def test_duplicate_eligibility_resend_scenario_creates_issue_case_and_sla():
    expected_date = "2028-01-20"
    result = run(expected_date=expected_date)

    issue = fetch_one(
        """
        SELECT issue_id, issue_code, status, file_id, run_id
        FROM data_quality_issues
        WHERE issue_id = ?
        """,
        (result["issue_id"],),
    )
    assert issue is not None
    assert issue["issue_code"] == "DUPLICATE_ELIGIBILITY_RESEND"
    assert issue["status"] == "OPEN"

    duplicate_file = fetch_one(
        """
        SELECT file_id, duplicate_flag, processing_status, file_hash
        FROM inbound_files
        WHERE file_id = ?
        """,
        (result["duplicate_file_id"],),
    )
    assert duplicate_file is not None
    assert duplicate_file["duplicate_flag"] == 1
    assert duplicate_file["processing_status"] == "REJECTED"

    support_case = fetch_one(
        """
        SELECT case_id, issue_id, assigned_team
        FROM support_cases
        WHERE issue_id = ?
        """,
        (result["issue_id"],),
    )
    assert support_case is not None
    assert support_case["issue_id"] == result["issue_id"]

    sla = fetch_one(
        """
        SELECT sla_id, case_id, target_hours
        FROM sla_tracking
        WHERE case_id = ?
        """,
        (support_case["case_id"],),
    )
    assert sla is not None
    assert sla["target_hours"] == 24


def test_duplicate_eligibility_resend_scenario_is_idempotent_for_same_open_incident():
    expected_date = "2028-01-21"

    first = run(expected_date=expected_date)
    second = run(expected_date=expected_date)

    issues = fetch_all(
        """
        SELECT issue_id
        FROM data_quality_issues
        WHERE issue_code = 'DUPLICATE_ELIGIBILITY_RESEND'
          AND client_id = ?
          AND vendor_id = ?
          AND status = 'OPEN'
          AND entity_key = ?
        """,
        (
            first["client_id"],
            first["vendor_id"],
            f"{expected_date}|{first['file_hash']}",
        ),
    )

    assert len(issues) == 1