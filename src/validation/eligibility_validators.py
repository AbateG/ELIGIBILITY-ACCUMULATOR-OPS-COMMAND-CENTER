from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from src.issues.issue_service import create_issue
from src.validation._common import _build_issue, _is_valid_date, _normalize_string, _normalize_upper, _parse_date

ALLOWED_RELATIONSHIP_CODES = {"SUB", "SPOUSE", "CHILD", "SELF"}
ALLOWED_ELIGIBILITY_STATUSES = {"ACTIVE", "TERMINATED", "PENDING", "COBRA"}


def validate_eligibility_row_schema(row: pd.Series) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    critical_fields = [
        "client_code",
        "vendor_code",
        "subscriber_id",
        "member_id",
        "plan_code",
        "coverage_start",
        "status",
    ]

    for field in critical_fields:
        if pd.isna(row.get(field)) or _normalize_string(row.get(field)) == "":
            issues.append(
                _build_issue(
                    issue_type="SCHEMA",
                    issue_subtype=f"NULL_{field.upper()}",
                    severity="HIGH",
                    issue_description=f"Required field {field} is null or empty",
                )
            )

    if _normalize_string(row.get("dob")) and not _is_valid_date(row.get("dob")):
        issues.append(
            _build_issue(
                issue_type="SCHEMA",
                issue_subtype="INVALID_DOB",
                severity="MEDIUM",
                issue_description=f"Invalid DOB: {row.get('dob')}",
            )
        )

    if _normalize_string(row.get("coverage_start")) and not _is_valid_date(row.get("coverage_start")):
        issues.append(
            _build_issue(
                issue_type="SCHEMA",
                issue_subtype="INVALID_COVERAGE_START",
                severity="HIGH",
                issue_description=f"Invalid coverage_start: {row.get('coverage_start')}",
            )
        )

    if _normalize_string(row.get("coverage_end")) and not _is_valid_date(row.get("coverage_end")):
        issues.append(
            _build_issue(
                issue_type="SCHEMA",
                issue_subtype="INVALID_COVERAGE_END",
                severity="MEDIUM",
                issue_description=f"Invalid coverage_end: {row.get('coverage_end')}",
            )
        )

    relationship_code = _normalize_upper(row.get("relationship_code"))
    if relationship_code and relationship_code not in ALLOWED_RELATIONSHIP_CODES:
        issues.append(
            _build_issue(
                issue_type="SCHEMA",
                issue_subtype="INVALID_RELATIONSHIP_CODE",
                severity="MEDIUM",
                issue_description=f"Invalid relationship_code: {row.get('relationship_code')}",
            )
        )

    status = _normalize_upper(row.get("status"))
    if status and status not in ALLOWED_ELIGIBILITY_STATUSES:
        issues.append(
            _build_issue(
                issue_type="SCHEMA",
                issue_subtype="INVALID_ELIGIBILITY_STATUS",
                severity="MEDIUM",
                issue_description=f"Invalid status: {row.get('status')}",
            )
        )

    start_dt = _parse_date(row.get("coverage_start"))
    end_dt = _parse_date(row.get("coverage_end"))
    if start_dt and end_dt and end_dt < start_dt:
        issues.append(
            _build_issue(
                issue_type="BUSINESS_RULE",
                issue_subtype="INVALID_COVERAGE_RANGE",
                severity="HIGH",
                issue_description=(
                    f"coverage_end {row.get('coverage_end')} is before "
                    f"coverage_start {row.get('coverage_start')}"
                ),
            )
        )

    return issues


def validate_eligibility_row_referential(
    row: pd.Series,
    ref_data: dict[str, Any],
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    client_code = _normalize_string(row.get("client_code"))
    vendor_code = _normalize_string(row.get("vendor_code"))
    member_id = _normalize_string(row.get("member_id"))
    subscriber_id = _normalize_string(row.get("subscriber_id"))
    plan_code = _normalize_string(row.get("plan_code"))
    relationship_code = _normalize_upper(row.get("relationship_code"))

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
    else:
        member_info = ref_data["member_map"].get(member_id)
        if member_info:
            expected_client_code = member_info["client_code"]
            expected_subscriber_id = member_info["subscriber_id"]
            expected_relationship = _normalize_upper(member_info["relationship_code"])

            if client_code and client_code != expected_client_code:
                issues.append(
                    _build_issue(
                        issue_type="REFERENTIAL",
                        issue_subtype="MEMBER_CLIENT_MISMATCH",
                        severity="HIGH",
                        issue_description=(
                            f"Member {member_id} belongs to client "
                            f"{expected_client_code}, not {client_code}"
                        ),
                    )
                )

            if subscriber_id and subscriber_id != expected_subscriber_id:
                issues.append(
                    _build_issue(
                        issue_type="REFERENTIAL",
                        issue_subtype="INVALID_SUBSCRIBER_LINK",
                        severity="HIGH",
                        issue_description=(
                            f"Member {member_id} is linked to subscriber "
                            f"{expected_subscriber_id}, not {subscriber_id}"
                        ),
                    )
                )

            if relationship_code and relationship_code != expected_relationship:
                issues.append(
                    _build_issue(
                        issue_type="REFERENTIAL",
                        issue_subtype="RELATIONSHIP_MISMATCH",
                        severity="MEDIUM",
                        issue_description=(
                            f"Member {member_id} has relationship "
                            f"{expected_relationship}, not {relationship_code}"
                        ),
                    )
                )

    return issues


def validate_duplicate_eligibility_rows(
    eligibility_rows: list[pd.Series],
) -> list[dict[str, Any]]:
    """
    Detect exact duplicate eligibility rows within the same file/batch.
    """
    issues: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str]] = set()

    for row_index, row in enumerate(eligibility_rows, start=2):
        key = (
            _normalize_string(row.get("member_id")),
            _normalize_string(row.get("subscriber_id")),
            _normalize_string(row.get("plan_code")),
            _normalize_string(row.get("coverage_start")),
            _normalize_string(row.get("coverage_end")),
            _normalize_upper(row.get("status")),
        )

        if not key[0] or not key[2] or not key[3]:
            continue

        if key in seen:
            issues.append(
                {
                    "row_number": row_index,
                    "issue": _build_issue(
                        issue_type="ELIGIBILITY",
                        issue_subtype="DUPLICATE_ELIGIBILITY_ROW",
                        severity="MEDIUM",
                        issue_description=(
                            "Duplicate eligibility row detected for "
                            f"member_id={key[0]}, plan_code={key[2]}, "
                            f"coverage_start={key[3]}, coverage_end={key[4] or 'OPEN'}"
                        ),
                    ),
                }
            )
        else:
            seen.add(key)

    return issues


def validate_eligibility_period_conflicts(
    eligibility_rows: list[pd.Series],
) -> list[dict[str, Any]]:
    """
    Detect cross-row period problems for the same member and plan:
    - overlapping coverage periods
    - gaps in coverage periods
    """
    issues: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[tuple[int, pd.Series]]] = {}

    for row_index, row in enumerate(eligibility_rows, start=2):
        member_id = _normalize_string(row.get("member_id"))
        plan_code = _normalize_string(row.get("plan_code"))
        start_dt = _parse_date(row.get("coverage_start"))

        if not member_id or not plan_code or start_dt is None:
            continue

        grouped.setdefault((member_id, plan_code), []).append((row_index, row))

    for (member_id, plan_code), grouped_rows in grouped.items():
        sorted_rows = sorted(
            grouped_rows,
            key=lambda item: (
                _parse_date(item[1].get("coverage_start")) or datetime.max,
                _parse_date(item[1].get("coverage_end")) or datetime.max,
            ),
        )

        previous_end: datetime | None = None
        previous_row_number: int | None = None

        for row_number, row in sorted_rows:
            current_start = _parse_date(row.get("coverage_start"))
            current_end = _parse_date(row.get("coverage_end")) or datetime.max

            if current_start is None:
                continue

            if previous_end is not None:
                if current_start <= previous_end:
                    issues.append(
                        {
                            "row_number": row_number,
                            "issue": _build_issue(
                                issue_type="ELIGIBILITY",
                                issue_subtype="ELIGIBILITY_OVERLAP",
                                severity="HIGH",
                                issue_description=(
                                    f"Overlapping coverage period detected for member_id={member_id}, "
                                    f"plan_code={plan_code}; row {row_number} overlaps prior row "
                                    f"{previous_row_number}"
                                ),
                            ),
                        }
                    )
                elif (current_start - previous_end).days > 1:
                    issues.append(
                        {
                            "row_number": row_number,
                            "issue": _build_issue(
                                issue_type="ELIGIBILITY",
                                issue_subtype="ELIGIBILITY_GAP",
                                severity="MEDIUM",
                                issue_description=(
                                    f"Coverage gap detected for member_id={member_id}, "
                                    f"plan_code={plan_code}; row {row_number} starts after a gap "
                                    f"from prior row {previous_row_number}"
                                ),
                            ),
                        }
                    )

            previous_end = current_end
            previous_row_number = row_number

    return issues


def create_row_issues(
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
    raw_member_id = row.get("member_id")
    normalized_member_id = None
    member_id_valid = None

    if pd.notna(raw_member_id):
        normalized_member_id = str(raw_member_id).strip() or None
        if normalized_member_id and normalized_member_id in ref_data["member_map"]:
            member_id_valid = normalized_member_id

    entity_key = normalized_member_id

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
            entity_name="eligibility_periods",
            entity_key=entity_key,
            source_row_number=row_number,
            issue_description=issue["issue_description"],
        )
        created_issue_ids.append(issue_id)

    return created_issue_ids