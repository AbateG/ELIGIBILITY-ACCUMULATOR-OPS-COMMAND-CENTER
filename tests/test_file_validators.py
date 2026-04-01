from pathlib import Path

import pandas as pd

from src.validation.file_validators import (
    validate_csv_readable,
    validate_file_name_pattern,
    validate_file_not_empty,
    validate_required_columns,
)


def test_validate_file_name_pattern_for_valid_eligibility_file():
    ok, msg = validate_file_name_pattern("ELIG_ACME_TPA1_20250101.csv", "ELIGIBILITY")
    assert ok is True
    assert msg is None


def test_validate_file_name_pattern_for_invalid_eligibility_file():
    ok, msg = validate_file_name_pattern("bad_file_name.csv", "ELIGIBILITY")
    assert ok is False
    assert "naming convention" in msg.lower()


def test_validate_file_not_empty(tmp_path: Path):
    file_path = tmp_path / "sample.csv"
    file_path.write_text("a,b\n1,2\n", encoding="utf-8")
    ok, msg = validate_file_not_empty(file_path)
    assert ok is True
    assert msg is None


def test_validate_empty_file(tmp_path: Path):
    file_path = tmp_path / "empty.csv"
    file_path.write_text("", encoding="utf-8")
    ok, msg = validate_file_not_empty(file_path)
    assert ok is False
    assert "empty" in msg.lower()


def test_validate_csv_readable(tmp_path: Path):
    file_path = tmp_path / "sample.csv"
    pd.DataFrame([{"col1": 1, "col2": 2}]).to_csv(file_path, index=False)
    ok, msg = validate_csv_readable(file_path)
    assert ok is True
    assert msg is None


def test_validate_required_columns_for_eligibility(tmp_path: Path):
    file_path = tmp_path / "elig.csv"
    df = pd.DataFrame(
        [
            {
                "record_id": "1",
                "client_code": "ACME",
                "vendor_code": "TPA1",
                "subscriber_id": "S0001",
                "member_id": "S0001-M1",
                "relationship_code": "SUB",
                "first_name": "John",
                "last_name": "Doe",
                "dob": "1980-01-01",
                "gender": "M",
                "plan_code": "PPO_STD_2025",
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "group_id": "ACME_GRP_001",
            }
        ]
    )
    df.to_csv(file_path, index=False)

    ok, msg = validate_required_columns(file_path, "ELIGIBILITY")
    assert ok is True
    assert msg is None


def test_validate_required_columns_missing_for_eligibility(tmp_path: Path):
    file_path = tmp_path / "elig_missing.csv"
    df = pd.DataFrame(
        [
            {
                "record_id": "1",
                "client_code": "ACME",
                "member_id": "S0001-M1",
            }
        ]
    )
    df.to_csv(file_path, index=False)

    ok, msg = validate_required_columns(file_path, "ELIGIBILITY")
    assert ok is False
    assert "missing required columns" in msg.lower()