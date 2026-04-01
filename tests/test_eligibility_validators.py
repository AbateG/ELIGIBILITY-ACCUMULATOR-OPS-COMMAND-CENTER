import pandas as pd

from src.validation.eligibility_validators import (
    validate_duplicate_eligibility_rows,
    validate_eligibility_period_conflicts,
    validate_eligibility_row_schema,
)


def make_eligibility_row(**overrides) -> pd.Series:
    base = {
        "client_code": "CLIENT_A",
        "vendor_code": "VENDOR_X",
        "subscriber_id": "S001",
        "member_id": "M001",
        "plan_code": "PLAN_100",
        "coverage_start": "2025-01-01",
        "coverage_end": "2025-12-31",
        "status": "ACTIVE",
        "relationship_code": "SUB",
        "dob": "1990-05-20",
    }
    base.update(overrides)
    return pd.Series(base)


def test_validate_eligibility_row_schema_valid_row_has_no_issues():
    row = make_eligibility_row()
    issues = validate_eligibility_row_schema(row)
    assert issues == []


def test_validate_eligibility_row_schema_invalid_status():
    row = make_eligibility_row(status="BAD_STATUS")
    issues = validate_eligibility_row_schema(row)

    assert len(issues) == 1
    assert issues[0]["issue_subtype"] == "INVALID_ELIGIBILITY_STATUS"


def test_validate_eligibility_row_schema_invalid_relationship_code():
    row = make_eligibility_row(relationship_code="OTHER")
    issues = validate_eligibility_row_schema(row)

    assert len(issues) == 1
    assert issues[0]["issue_subtype"] == "INVALID_RELATIONSHIP_CODE"


def test_validate_eligibility_row_schema_invalid_coverage_range():
    row = make_eligibility_row(
        coverage_start="2025-12-31",
        coverage_end="2025-01-01",
    )
    issues = validate_eligibility_row_schema(row)

    assert any(issue["issue_subtype"] == "INVALID_COVERAGE_RANGE" for issue in issues)


def test_validate_duplicate_eligibility_rows_no_duplicates():
    rows = [
        make_eligibility_row(member_id="M001"),
        make_eligibility_row(member_id="M002"),
    ]
    issues = validate_duplicate_eligibility_rows(rows)
    assert issues == []


def test_validate_duplicate_eligibility_rows_detects_duplicate():
    rows = [
        make_eligibility_row(member_id="M001"),
        make_eligibility_row(member_id="M001"),
    ]
    issues = validate_duplicate_eligibility_rows(rows)

    assert len(issues) == 1
    assert issues[0]["issue"]["issue_subtype"] == "DUPLICATE_ELIGIBILITY_ROW"
    assert issues[0]["row_number"] == 3


def test_validate_eligibility_period_conflicts_no_conflict_for_contiguous_periods():
    rows = [
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_100",
            coverage_start="2025-01-01",
            coverage_end="2025-06-30",
        ),
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_100",
            coverage_start="2025-07-01",
            coverage_end="2025-12-31",
        ),
    ]

    issues = validate_eligibility_period_conflicts(rows)
    assert issues == []


def test_validate_eligibility_period_conflicts_detects_overlap():
    rows = [
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_100",
            coverage_start="2025-01-01",
            coverage_end="2025-06-30",
        ),
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_100",
            coverage_start="2025-06-15",
            coverage_end="2025-12-31",
        ),
    ]

    issues = validate_eligibility_period_conflicts(rows)

    assert len(issues) == 1
    assert issues[0]["issue"]["issue_subtype"] == "ELIGIBILITY_OVERLAP"
    assert issues[0]["row_number"] == 3


def test_validate_eligibility_period_conflicts_detects_gap():
    rows = [
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_100",
            coverage_start="2025-01-01",
            coverage_end="2025-06-30",
        ),
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_100",
            coverage_start="2025-07-10",
            coverage_end="2025-12-31",
        ),
    ]

    issues = validate_eligibility_period_conflicts(rows)

    assert len(issues) == 1
    assert issues[0]["issue"]["issue_subtype"] == "ELIGIBILITY_GAP"
    assert issues[0]["row_number"] == 3


def test_validate_eligibility_period_conflicts_separate_plans_do_not_conflict():
    rows = [
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_A",
            coverage_start="2025-01-01",
            coverage_end="2025-06-30",
        ),
        make_eligibility_row(
            member_id="M001",
            plan_code="PLAN_B",
            coverage_start="2025-03-01",
            coverage_end="2025-12-31",
        ),
    ]

    issues = validate_eligibility_period_conflicts(rows)
    assert issues == []