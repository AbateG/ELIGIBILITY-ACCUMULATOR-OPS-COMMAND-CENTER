"""
Shared test utilities for the eligibility accumulator test suite.
"""

import csv
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def generate_test_id(prefix: str = "TEST") -> str:
    """
    Generate a unique test ID to avoid conflicts in parallel test execution.
    
    Args:
        prefix: Prefix for the test ID
        
    Returns:
        Unique test identifier
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"{prefix}_{timestamp}"


def write_csv(
    rows: list[dict[str, Any]],
    columns: list[str],
    filepath: str | Path,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> None:
    """
    Write rows to a CSV file.
    
    Args:
        rows: List of dictionaries representing rows
        columns: Column names in order
        filepath: Path to write CSV file
        delimiter: CSV delimiter
        encoding: File encoding
    """
    with open(filepath, "w", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter=delimiter)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def create_temp_csv(
    rows: list[dict[str, Any]],
    columns: list[str],
    suffix: str = ".csv",
) -> Path:
    """
    Create a temporary CSV file for testing.
    
    Args:
        rows: List of dictionaries representing rows
        columns: Column names in order
        suffix: File suffix
        
    Returns:
        Path to temporary file (automatically cleaned up when closed)
    """
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False)
    temp_path = Path(temp_file.name)
    
    try:
        write_csv(rows, columns, temp_path)
    finally:
        temp_file.close()
    
    return temp_path


def assert_dicts_equal(
    actual: dict[str, Any],
    expected: dict[str, Any],
    ignore_keys: list[str] | None = None,
    msg: str = "Dictionaries do not match",
) -> None:
    """
    Assert that two dictionaries are equal, optionally ignoring specific keys.
    
    Args:
        actual: Actual dictionary
        expected: Expected dictionary
        ignore_keys: Keys to ignore in comparison
        msg: Assertion message
    """
    ignore_keys = ignore_keys or []
    
    actual_filtered = {k: v for k, v in actual.items() if k not in ignore_keys}
    expected_filtered = {k: v for k, v in expected.items() if k not in ignore_keys}
    
    assert actual_filtered == expected_filtered, f"{msg}\nActual: {actual_filtered}\nExpected: {expected_filtered}"


def assert_dataframe_equal(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    check_column_order: bool = False,
    check_index: bool = False,
    msg: str = "DataFrames do not match",
) -> None:
    """
    Assert that two DataFrames are equal with helpful error messages.
    
    Args:
        actual: Actual DataFrame
        expected: Expected DataFrame
        check_column_order: Whether to check column order
        check_index: Whether to check index equality
        msg: Assertion message
    """
    if check_column_order:
        assert list(actual.columns) == list(expected.columns), (
            f"{msg}: Column order mismatch\n"
            f"Actual columns: {list(actual.columns)}\n"
            f"Expected columns: {list(expected.columns)}"
        )
    
    if check_index:
        assert actual.index.equals(expected.index), (
            f"{msg}: Index mismatch\n"
            f"Actual index: {actual.index}\n"
            f"Expected index: {expected.index}"
        )
    
    pd.testing.assert_frame_equal(
        actual,
        expected,
        check_dtype=False,
        check_index=check_index,
        check_column_type=check_column_order,
    )


class TestDatabaseManager:
    """
    Context manager for test database operations.
    
    Ensures proper cleanup of database connections and environment variables.
    """
    
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.old_db_path = None
        
    def __enter__(self) -> "TestDatabaseManager":
        import os
        self.old_db_path = os.environ.get("SQLITE_DB_PATH")
        os.environ["SQLITE_DB_PATH"] = str(self.db_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        import os
        if self.old_db_path is not None:
            os.environ["SQLITE_DB_PATH"] = self.old_db_path
        else:
            os.environ.pop("SQLITE_DB_PATH", None)


def seed_minimal_test_data(conn) -> dict[str, Any]:
    """
    Seed minimal reference data for tests.
    
    Args:
        conn: Database connection
        
    Returns:
        Dictionary of seeded reference data
    """
    # Basic test data - much smaller than conftest.py
    conn.execute("""
        INSERT OR IGNORE INTO clients (client_id, client_code, client_name, active_flag, created_at)
        VALUES (1, 'TEST', 'Test Client', 1, '2025-01-01T00:00:00')
    """)
    
    conn.execute("""
        INSERT OR IGNORE INTO vendors (vendor_id, vendor_code, vendor_name, vendor_type, active_flag, created_at)
        VALUES (1, 'TEST-VEN', 'Test Vendor', 'TPA', 1, '2025-01-01T00:00:00')
    """)
    
    conn.execute("""
        INSERT OR IGNORE INTO benefit_plans (
            plan_id, plan_code, plan_name, plan_type, client_id, benefit_year,
            individual_deductible, family_deductible,
            individual_oop_max, family_oop_max,
            coinsurance_rate, active_flag
        )
        VALUES (1, 'TEST-PLAN', 'Test Plan', 'PPO', 1, 2025, 1000.00, 2000.00, 5000.00, 10000.00, 0.20, 1)
    """)
    
    # Minimal member data
    members_data = [
        ('MBR-001', 'SUB-001', 1, 'Test', 'User', '1990-01-01', 'M', 'SUB', 'FAM-001'),
        ('MBR-002', 'SUB-001', 1, 'Test', 'Spouse', '1992-01-01', 'F', 'SPOUSE', 'FAM-001'),
    ]
    
    conn.executemany("""
        INSERT OR IGNORE INTO members (
            member_id, subscriber_id, client_id, first_name, last_name, dob, gender,
            relationship_code, family_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, members_data)
    
    return {
        "client_codes": ["TEST"],
        "vendor_codes": ["TEST-VEN"],
        "plan_codes": ["TEST-PLAN"],
        "member_map": {
            "MBR-001": {
                "client_code": "TEST",
                "subscriber_id": "SUB-001",
                "relationship_code": "SUB"
            },
            "MBR-002": {
                "client_code": "TEST",
                "subscriber_id": "SUB-001",
                "relationship_code": "SPOUSE"
            }
        }
    }