from src.common.db import fetch_all, fetch_one
from src.scenarios.scenario_missing_inbound_file import run


def test_missing_inbound_file_scenario_creates_issue_case_and_sla():
    expected_date = "2028-01-15"
    result = run(expected_date=expected_date)

    issue = fetch_one(
        """
        SELECT issue_id, issue_code, status, file_id, run_id, entity_key
        FROM data_quality_issues
        WHERE issue_id = ?
        """,
        (result["issue_id"],),
    )
    assert issue is not None
    assert issue["issue_code"] == "MISSING_INBOUND_FILE"
    assert issue["status"] == "OPEN"
    assert issue["entity_key"] == expected_date

    support_case = fetch_one(
        """
        SELECT case_id, issue_id, status, assigned_team, priority
        FROM support_cases
        WHERE issue_id = ?
        """,
        (result["issue_id"],),
    )
    assert support_case is not None
    assert support_case["issue_id"] == result["issue_id"]
    assert support_case["assigned_team"] == "ops_file_queue"

    sla = fetch_one(
        """
        SELECT sla_id, case_id, target_hours, status
        FROM sla_tracking
        WHERE case_id = ?
        """,
        (support_case["case_id"],),
    )
    assert sla is not None
    assert sla["target_hours"] == 4
    assert sla["status"] == "OPEN"


def test_missing_inbound_file_scenario_is_idempotent_for_same_open_incident():
    expected_date = "2028-01-16"

    first = run(expected_date=expected_date)
    second = run(expected_date=expected_date)

    issues = fetch_all(
        """
        SELECT issue_id
        FROM data_quality_issues
        WHERE issue_code = 'MISSING_INBOUND_FILE'
          AND client_id = ?
          AND vendor_id = ?
          AND status = 'OPEN'
          AND entity_key = ?
        """,
        (first["client_id"], first["vendor_id"], expected_date),
    )

    assert len(issues) == 1