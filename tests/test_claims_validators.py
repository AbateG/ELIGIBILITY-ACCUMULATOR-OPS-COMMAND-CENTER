import pandas as pd

from src.validation.claims_validators import (
    validate_claim_amount_relationships,
    validate_claim_row_schema,
    validate_duplicate_claim_rows,
)


def make_claim_row(**overrides) -> pd.Series:
    base = {
        "claim_id": "C001",
        "line_id": "1",
        "client_code": "CLIENT_A",
        "vendor_code": "VENDOR_X",
        "member_id": "M001",
        "subscriber_id": "S001",
        "plan_code": "PLAN_100",
        "service_date": "2025-01-15",
        "paid_date": "2025-01-20",
        "allowed_amount": 100.00,
        "paid_amount": 80.00,
        "member_responsibility": 20.00,
        "deductible_amount": 10.00,
        "coinsurance_amount": 5.00,
        "copay_amount": 5.00,
        "claim_status": "PAID",
        "preventive_flag": "0",
    }
    base.update(overrides)
    return pd.Series(base)


def test_validate_claim_row_schema_valid_row_has_no_issues():
    row = make_claim_row()
    issues = validate_claim_row_schema(row)
    assert issues == []


def test_validate_claim_row_schema_invalid_claim_status():
    row = make_claim_row(claim_status="weird_status")
    issues = validate_claim_row_schema(row)

    assert len(issues) == 1
    assert issues[0]["issue_subtype"] == "INVALID_CLAIM_STATUS"


def test_validate_claim_row_schema_negative_amount_non_reversed():
    row = make_claim_row(paid_amount=-5.00, claim_status="PAID")
    issues = validate_claim_row_schema(row)

    assert any(issue["issue_subtype"] == "INVALID_NEGATIVE_AMOUNT" for issue in issues)


def test_validate_claim_row_schema_negative_amount_reversed_allowed():
    row = make_claim_row(paid_amount=-5.00, claim_status="REVERSED")
    issues = validate_claim_row_schema(row)

    assert not any(issue["issue_subtype"] == "INVALID_NEGATIVE_AMOUNT" for issue in issues)


def test_validate_claim_row_schema_preventive_deductible_applied():
    row = make_claim_row(preventive_flag="True", deductible_amount=25.00)
    issues = validate_claim_row_schema(row)

    assert any(issue["issue_subtype"] == "PREVENTIVE_DED_APPLIED" for issue in issues)


def test_validate_claim_amount_relationships_valid_row():
    row = make_claim_row()
    issues = validate_claim_amount_relationships(row)
    assert issues == []


def test_validate_claim_amount_relationships_paid_exceeds_allowed():
    row = make_claim_row(allowed_amount=100.00, paid_amount=120.00)
    issues = validate_claim_amount_relationships(row)

    assert any(issue["issue_subtype"] == "PAID_EXCEEDS_ALLOWED" for issue in issues)


def test_validate_claim_amount_relationships_member_resp_exceeds_allowed():
    row = make_claim_row(allowed_amount=100.00, member_responsibility=120.00)
    issues = validate_claim_amount_relationships(row)

    assert any(issue["issue_subtype"] == "MEMBER_RESP_EXCEEDS_ALLOWED" for issue in issues)


def test_validate_claim_amount_relationships_component_exceeds_member_resp():
    row = make_claim_row(member_responsibility=20.00, deductible_amount=25.00)
    issues = validate_claim_amount_relationships(row)

    assert any(issue["issue_subtype"] == "COMPONENT_EXCEEDS_MEMBER_RESP" for issue in issues)


def test_validate_claim_amount_relationships_member_resp_component_mismatch():
    row = make_claim_row(
        member_responsibility=20.00,
        deductible_amount=10.00,
        coinsurance_amount=10.00,
        copay_amount=10.00,
    )
    issues = validate_claim_amount_relationships(row)

    assert any(issue["issue_subtype"] == "MEMBER_RESP_COMPONENT_MISMATCH" for issue in issues)


def test_validate_duplicate_claim_rows_no_duplicates():
    rows = [
        make_claim_row(claim_id="C001", line_id="1"),
        make_claim_row(claim_id="C002", line_id="1"),
    ]

    issues = validate_duplicate_claim_rows(rows)
    assert issues == []


def test_validate_duplicate_claim_rows_exact_duplicate():
    rows = [
        make_claim_row(claim_id="C001", line_id="1"),
        make_claim_row(claim_id="C001", line_id="1"),
    ]

    issues = validate_duplicate_claim_rows(rows)

    assert len(issues) == 1
    assert issues[0]["issue"]["issue_subtype"] == "DUPLICATE_CLAIM_EXACT"
    assert issues[0]["row_number"] == 3


def test_validate_duplicate_claim_rows_conflicting_duplicate():
    rows = [
        make_claim_row(claim_id="C001", line_id="1", paid_amount=80.00),
        make_claim_row(claim_id="C001", line_id="1", paid_amount=90.00),
    ]

    issues = validate_duplicate_claim_rows(rows)

    assert len(issues) == 1
    assert issues[0]["issue"]["issue_subtype"] == "DUPLICATE_CLAIM_CONFLICT"
    assert issues[0]["row_number"] == 3