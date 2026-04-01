from src.common.db import fetch_all, fetch_one
from src.scenarios.scenario_claim_for_ineligible_member import run


def test_claim_for_ineligible_member_scenario_creates_issue_case_and_sla():
    service_date = "2028-01-26"
    result = run(service_date=service_date)

    issue = fetch_one(
        """
        SELECT *
        FROM data_quality_issues
        WHERE issue_id = ?
        """,
        (result["issue_id"],),
    )
    assert issue is not None
    assert issue["issue_code"] == "CLAIM_INELIGIBLE_MEMBER"
    assert issue["status"] == "OPEN"
    assert issue["claim_record_id"] == result["claim_record_id"]
    assert issue["entity_key"] == service_date

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
    assert sla["target_hours"] == 8


def test_claim_for_ineligible_member_scenario_is_idempotent_for_same_open_incident():
    service_date = "2028-01-26"

    first = run(service_date=service_date)
    second = run(service_date=service_date)

    issues = fetch_all(
        """
        SELECT issue_id
        FROM data_quality_issues
        WHERE issue_code = 'CLAIM_INELIGIBLE_MEMBER'
          AND member_id = ?
          AND status = 'OPEN'
          AND entity_key = ?
        """,
        (first["member_id"], service_date),
    )

    assert len(issues) == 1