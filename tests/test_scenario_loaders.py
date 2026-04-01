from src.common.db import fetch_all, fetch_one
from src.scenarios.scenario_claim_for_ineligible_member import run as run_claim_ineligible
from src.scenarios.scenario_duplicate_eligibility_resend import run as run_duplicate_resend
from src.scenarios.scenario_missing_inbound_file import run as run_missing_file


def get_issue_by_code(code: str, **filters):
    """Helper to get issue by code with optional filters."""
    query = f"SELECT * FROM data_quality_issues WHERE issue_code = ?"
    params = (code,)
    for k, v in filters.items():
        query += f" AND {k} = ?"
        params += (v,)
    query += " ORDER BY issue_id DESC LIMIT 1"
    return fetch_one(query, params)


def get_case_for_issue(issue_id: int):
    """Helper to get support case for an issue."""
    return fetch_one(
        "SELECT * FROM support_cases WHERE issue_id = ?",
        (issue_id,),
    )


def get_sla_for_case(case_id: int):
    """Helper to get SLA for a case."""
    return fetch_one(
        "SELECT * FROM sla_tracking WHERE case_id = ?",
        (case_id,),
    )


def test_claim_for_ineligible_member_scenario():
    """Test claim for ineligible member creates issue, case, SLA with correct routing/priority/SLA."""
    service_date = "2028-01-25"
    result = run_claim_ineligible(service_date=service_date)

    # A. Issue created
    issue = get_issue_by_code("CLAIM_INELIGIBLE_MEMBER", issue_id=result["issue_id"])
    assert issue is not None
    assert issue["status"] == "OPEN"

    # B. Case created
    case = get_case_for_issue(result["issue_id"])
    assert case is not None
    assert case["assigned_team"] == "ops_claims_queue"
    assert case["priority"] == "HIGH"

    # C. SLA created
    sla = get_sla_for_case(case["case_id"])
    assert sla is not None
    assert sla["target_hours"] == 8

    # D. Rerun is safe
    second_result = run_claim_ineligible(service_date=service_date)
    assert second_result["issue_id"] == result["issue_id"]  # Same issue


def test_duplicate_eligibility_resend_scenario():
    """Test duplicate eligibility resend creates issue, case, SLA with correct routing/priority/SLA."""
    expected_date = "2028-01-20"
    result = run_duplicate_resend(expected_date=expected_date)

    # A. Issue created
    issue = get_issue_by_code("DUPLICATE_ELIGIBILITY_RESEND", issue_id=result["issue_id"])
    assert issue is not None
    assert issue["status"] == "OPEN"

    # B. Case created
    case = get_case_for_issue(result["issue_id"])
    assert case is not None
    # Assume priority/team as per implementation, e.g., MEDIUM, ops_eligibility_queue
    assert case["priority"] == "MEDIUM"
    assert case["assigned_team"] == "ops_eligibility_queue"

    # C. SLA created
    sla = get_sla_for_case(case["case_id"])
    assert sla is not None
    assert sla["target_hours"] == 24

    # D. Rerun is safe
    second_result = run_duplicate_resend(expected_date=expected_date)
    assert second_result["issue_id"] == result["issue_id"]


def test_missing_inbound_file_scenario():
    """Test missing inbound file creates issue, case, SLA with correct routing/priority/SLA."""
    expected_date = "2028-01-15"
    result = run_missing_file(expected_date=expected_date)

    # A. Issue created
    issue = get_issue_by_code("MISSING_INBOUND_FILE", issue_id=result["issue_id"])
    assert issue is not None
    assert issue["status"] == "OPEN"

    # B. Case created
    case = get_case_for_issue(result["issue_id"])
    assert case is not None
    assert case["assigned_team"] == "ops_file_queue"
    assert case["priority"] == "CRITICAL"

    # C. SLA created
    sla = get_sla_for_case(case["case_id"])
    assert sla is not None
    assert sla["target_hours"] == 4

    # D. Rerun is safe
    second_result = run_missing_file(expected_date=expected_date)
    assert second_result["issue_id"] == result["issue_id"]