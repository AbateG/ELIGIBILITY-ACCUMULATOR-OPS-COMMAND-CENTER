from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd


def _build_issue(
    issue_type: str,
    issue_subtype: str,
    severity: str,
    issue_description: str,
) -> dict[str, str]:
    return {
        "issue_type": issue_type,
        "issue_subtype": issue_subtype,
        "severity": severity,
        "issue_description": issue_description,
    }


def _normalize_string(value: Any) -> str:
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


def _normalize_upper(value: Any) -> str:
    return _normalize_string(value).upper()


def _is_valid_date(value: Any) -> bool:
    normalized = _normalize_string(value)
    if not normalized:
        return False
    try:
        datetime.fromisoformat(normalized)
        return True
    except ValueError:
        return False


def _parse_date(value: Any) -> datetime | None:
    normalized = _normalize_string(value)
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None