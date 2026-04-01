from __future__ import annotations

from datetime import datetime
import math
from typing import Any

import pandas as pd

from src.issues.issue_service import create_issue
from src.validation._common import _build_issue, _is_valid_date, _normalize_string, _normalize_upper


ALLOWED_CLAIM_STATUSES = {"PAID", "DENIED", "ADJUSTED", "REVERSED"}

NUMERIC_CLAIM_FIELDS = [
    "allowed_amount",
    "paid_amount",
    "member_responsibility",
    "deductible_amount",
    "coinsurance_amount",
    "copay_amount",
]

CRITICAL_FIELDS = [
    "claim_id",
    "line_id",
    "client_code",
    "vendor_code",
    "member_id",
    "subscriber_id",
    "plan_code",
    "service_date",
    "allowed_amount",
    "paid_amount",
    "member_responsibility",
    "deductible_amount",
    "coinsurance_amount",
    "copay_amount",
    "claim_status",
]



def _to_float(value: Any) -> float | None:
    if pd.isna(value) or value in ("", None):
        return None
    try:
        numeric_value = float(value)
        if math.isnan(numeric_value) or math.isinf(numeric_value):
            return None
        return numeric_value
    except (TypeError, ValueError):
        return None


def _is_numeric(value: Any) -> bool:
    return _to_float(value) is not None


def _is_truthy_flag(value: Any) -> bool:
    return _normalize_upper(value) in {"1", "TRUE", "Y", "YES"}


def _is_reversed_claim(row: pd.Series) -> bool:
    return _normalize_upper(row.get("claim_status")) == "REVERSED"


def validate_claim_row_schema(row: pd.Series) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    for field in CRITICAL_FIELDS:
        value = row.get(field)
        if pd.isna(value) or _normalize_string(value) == "":
            issues.append(
                _build_issue(
                    issue_type="SCHEMA",
                    issue_subtype=f"NULL_{field.upper()}",
                    severity="HIGH",
                    issue_description=f"Required field {field} is null or empty",
                )
            )

    for field in ["service_date", "paid_date"]:
        value = row.get(field)
        if _normalize_string(value) and not _is_valid_date(value):
            issues.append(
                _build_issue(
                    issue_type="SCHEMA",
                    issue_subtype=f"INVALID_{field.upper()}",
                    severity="HIGH" if field == "service_date" else "MEDIUM",
                    issue_description=f"Invalid {field}: {value}",
                )
            )

    for field in NUMERIC_CLAIM_FIELDS:
        value = row.get(field)
        if not _is_numeric(value):
            # Skip if already flagged as null/empty for critical fields
            if field in CRITICAL_FIELDS and (pd.isna(value) or _normalize_string(value) == ""):
                continue
            issues.append(
                _build_issue(
                    issue_type="SCHEMA",
                    issue_subtype="INVALID_NUMERIC_FIELD",
                    severity="HIGH",
                    issue_description=f"Field {field} is not numeric: {value}",
                )
            )

    status = _normalize_upper(row.get("claim_status"))
    if status and status not in ALLOWED_CLAIM_STATUSES:
        issues.append(
            _build_issue(
                issue_type="SCHEMA",
                issue_subtype="INVALID_CLAIM_STATUS",
                severity="MEDIUM",
                issue_description=f"Invalid claim_status: {row.get('claim_status')}",
            )
        )

    reversal_context = _is_reversed_claim(row)
    for field in NUMERIC_CLAIM_FIELDS:
        numeric_value = _to_float(row.get(field))
        if numeric_value is not None and numeric_value < 0 and not reversal_context:
            issues.append(
                _build_issue(
                    issue_type="CLAIMS",
                    issue_subtype="INVALID_NEGATIVE_AMOUNT",
                    severity="HIGH",
                    issue_description=f"Negative value {numeric_value} in {field} without reversal context",
                )
            )

    preventive_flag = row.get("preventive_flag")
    deductible_amount = _to_float(row.get("deductible_amount"))
    if _is_truthy_flag(preventive_flag) and deductible_amount is not None and deductible_amount > 0:
        issues.append(
            _build_issue(
                issue_type="BUSINESS_RULE",
                issue_subtype="PREVENTIVE_DED_APPLIED",
                severity="MEDIUM",
                issue_description="Preventive claim has deductible amount greater than zero",
            )
        )

    return issues


def validate_claim_amount_relationships(row: pd.Series) -> list[dict[str, str]]:
    """
    Validate business relationships between claim financial fields.

    Rules:
    - paid_amount cannot exceed allowed_amount
    - member_responsibility cannot exceed allowed_amount
    - deductible/coinsurance/copay should not individually exceed member_responsibility
    - for non-reversed claims, expected member responsibility components should not exceed total responsibility
    """
    issues: list[dict[str, str]] = []

    allowed = _to_float(row.get("allowed_amount"))
    paid = _to_float(row.get("paid_amount"))
    member_resp = _to_float(row.get("member_responsibility"))
    deductible = _to_float(row.get("deductible_amount"))
    coinsurance = _to_float(row.get("coinsurance_amount"))
    copay = _to_float(row.get("copay_amount"))

    # Skip relationship checks if required numeric fields are missing/invalid.
    if allowed is None or paid is None or member_resp is None:
        return issues

    if paid > allowed:
        issues.append(
            _build_issue(
                issue_type="BUSINESS_RULE",
                issue_subtype="PAID_EXCEEDS_ALLOWED",
                severity="HIGH",
                issue_description=f"paid_amount {paid} exceeds allowed_amount {allowed}",
            )
        )

    if member_resp > allowed:
        issues.append(
            _build_issue(
                issue_type="BUSINESS_RULE",
                issue_subtype="MEMBER_RESP_EXCEEDS_ALLOWED",
                severity="HIGH",
                issue_description=f"member_responsibility {member_resp} exceeds allowed_amount {allowed}",
            )
        )

    component_values = {
        "deductible_amount": deductible,
        "coinsurance_amount": coinsurance,
        "copay_amount": copay,
    }

    for field_name, value in component_values.items():
        if value is not None and member_resp is not None and value > member_resp:
            issues.append(
                _build_issue(
                    issue_type="BUSINESS_RULE",
                    issue_subtype="COMPONENT_EXCEEDS_MEMBER_RESP",
                    severity="MEDIUM",
                    issue_description=f"{field_name} {value} exceeds member_responsibility {member_resp}",
                )
            )

    if deductible is not None and coinsurance is not None and copay is not None:
        component_total = deductible + coinsurance + copay
        if component_total > member_resp:
            issues.append(
                _build_issue(
                    issue_type="BUSINESS_RULE",
                    issue_subtype="MEMBER_RESP_COMPONENT_MISMATCH",
                    severity="MEDIUM",
                    issue_description=(
                        f"deductible + coinsurance + copay ({component_total}) "
                        f"exceeds member_responsibility ({member_resp})"
                    ),
                )
            )

    non_reversed = not _is_reversed_claim(row)
    if non_reversed and allowed < 0:
        issues.append(
            _build_issue(
                issue_type="BUSINESS_RULE",
                issue_subtype="INVALID_ALLOWED_AMOUNT",
                severity="HIGH",
                issue_description=f"allowed_amount {allowed} cannot be negative for non-reversed claim",
            )
        )

    return issues


def validate_duplicate_claim_rows(claim_rows: list[pd.Series]) -> list[dict[str, Any]]:
    """
    Detect duplicate claim rows within the same inbound file/batch.

    Duplicate key:
    - claim_id
    - line_id

    Behavior:
    - exact duplicate rows produce a low-severity issue
    - conflicting duplicates produce a high-severity issue
    """
    issues: list[dict[str, Any]] = []
    seen: dict[tuple[str, str], pd.Series] = {}

    compare_fields = [
        "member_id",
        "subscriber_id",
        "plan_code",
        "service_date",
        "allowed_amount",
        "paid_amount",
        "member_responsibility",
        "deductible_amount",
        "coinsurance_amount",
        "copay_amount",
        "claim_status",
    ]

    for row_index, row in enumerate(claim_rows, start=2):
        claim_id = _normalize_string(row.get("claim_id"))
        line_id = _normalize_string(row.get("line_id"))

        if not claim_id or not line_id:
            continue

        key = (claim_id, line_id)

        if key not in seen:
            seen[key] = row
            continue

        original_row = seen[key]
        differing_fields: list[str] = []

        for field in compare_fields:
            current_value = _normalize_string(row.get(field))
            original_value = _normalize_string(original_row.get(field))
            if current_value != original_value:
                differing_fields.append(field)

        if differing_fields:
            issues.append(
                {
                    "row_number": row_index,
                    "issue": _build_issue(
                        issue_type="CLAIMS",
                        issue_subtype="DUPLICATE_CLAIM_CONFLICT",
                        severity="HIGH",
                        issue_description=(
                            f"Duplicate claim detected for claim_id={claim_id}, "
                            f"line_id={line_id} with conflicting fields: {', '.join(differing_fields)}"
                        ),
                    ),
                }
            )
        else:
            issues.append(
                {
                    "row_number": row_index,
                    "issue": _build_issue(
                        issue_type="CLAIMS",
                        issue_subtype="DUPLICATE_CLAIM_EXACT",
                        severity="LOW",
                        issue_description=(
                            f"Exact duplicate claim row detected for claim_id={claim_id}, "
                            f"line_id={line_id}"
                        ),
                    ),
                }
            )

    return issues


def validate_claim_row_referential_and_eligibility(
    row: pd.Series,
    ref_data: dict[str, Any],
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    client_code = _normalize_string(row.get("client_code"))
    vendor_code = _normalize_string(row.get("vendor_code"))
    member_id = _normalize_string(row.get("member_id"))
    subscriber_id = _normalize_string(row.get("subscriber_id"))
    plan_code = _normalize_string(row.get("plan_code"))
    service_date = _normalize_string(row.get("service_date"))

    if client_code and client_code not in ref_data["client_codes"]:
        issues.append(
            _build_issue(
                issue_type="REFERENTIAL",
                issue_subtype="UNKNOWN_CLIENT",
                severity="HIGH",
                issue_description=f"Unknown client_code: {client_code}",
            )
        )

    if vendor_code and vendor_code not in ref_data["vendor_codes"]:
        issues.append(
            _build_issue(
                issue_type="REFERENTIAL",
                issue_subtype="UNKNOWN_VENDOR",
                severity="MEDIUM",
                issue_description=f"Unknown vendor_code: {vendor_code}",
            )
        )

    if plan_code and plan_code not in ref_data["plan_codes"]:
        issues.append(
            _build_issue(
                issue_type="REFERENTIAL",
                issue_subtype="UNKNOWN_PLAN",
                severity="HIGH",
                issue_description=f"Unknown plan_code: {plan_code}",
            )
        )

    if member_id and member_id not in ref_data["member_map"]:
        issues.append(
            _build_issue(
                issue_type="REFERENTIAL",
                issue_subtype="UNKNOWN_MEMBER",
                severity="HIGH",
                issue_description=f"Unknown member_id: {member_id}",
            )
        )
        return issues

    member_info = ref_data["member_map"].get(member_id)
    if member_info:
        if client_code and client_code != member_info["client_code"]:
            issues.append(
                _build_issue(
                    issue_type="REFERENTIAL",
                    issue_subtype="MEMBER_CLIENT_MISMATCH",
                    severity="HIGH",
                    issue_description=(
                        f"Member {member_id} belongs to client "
                        f"{member_info['client_code']}, not {client_code}"
                    ),
                )
            )

        if subscriber_id and subscriber_id != member_info["subscriber_id"]:
            issues.append(
                _build_issue(
                    issue_type="REFERENTIAL",
                    issue_subtype="INVALID_SUBSCRIBER_LINK",
                    severity="HIGH",
                    issue_description=(
                        f"Member {member_id} belongs to subscriber "
                        f"{member_info['subscriber_id']}, not {subscriber_id}"
                    ),
                )
            )

    if _is_valid_date(service_date) and member_id in ref_data["member_eligibility"]:
        service_dt = datetime.fromisoformat(service_date).date()
        eligible_segments = ref_data["member_eligibility"][member_id]

        match_found = False
        for seg in eligible_segments:
            start_dt = datetime.fromisoformat(seg["coverage_start"]).date()
            end_dt = (
                datetime.fromisoformat(seg["coverage_end"]).date()
                if seg["coverage_end"]
                else datetime(9999, 12, 31).date()
            )

            if start_dt <= service_dt <= end_dt and plan_code == seg["plan_code"]:
                match_found = True
                break

        if not match_found:
            issues.append(
                _build_issue(
                    issue_type="CLAIMS",
                    issue_subtype="INELIGIBLE_CLAIM",
                    severity="HIGH",
                    issue_description=(
                        f"Member {member_id} is not eligible for plan {plan_code} "
                        f"on service date {service_date}"
                    ),
                )
            )

    return issues


def create_claim_row_issues(
    conn,
    file_id: int,
    run_id: int,
    row_number: int,
    row: pd.Series,
    issues: list[dict[str, str]],
    client_id: int | None,
    vendor_id: int | None,
    ref_data: dict[str, Any],
) -> list[int]:
    created_issue_ids: list[int] = []
    member_id = row.get("member_id")
    member_id_valid = member_id if pd.notna(member_id) and member_id in ref_data["member_map"] else None
    entity_key = f"{row.get('claim_id', 'UNKNOWN')}|{row.get('line_id', 'UNKNOWN')}"

    for issue in issues:
        issue_id = create_issue(
            conn=conn,
            issue_type=issue["issue_type"],
            issue_subtype=issue["issue_subtype"],
            severity=issue["severity"],
            status="OPEN",
            file_id=file_id,
            run_id=run_id,
            member_id=member_id_valid,
            client_id=client_id,
            vendor_id=vendor_id,
            entity_name="claims",
            entity_key=entity_key,
            source_row_number=row_number,
            issue_description=issue["issue_description"],
        )
        created_issue_ids.append(issue_id)

    return created_issue_ids