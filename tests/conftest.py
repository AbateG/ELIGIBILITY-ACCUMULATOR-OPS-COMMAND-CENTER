"""
tests/conftest.py
Global test fixtures — every test gets an isolated temporary database
initialized via the canonical init_database() function.
"""

import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolated_test_db(request):
    """
    Automatically provide every test with an isolated temporary database.

    Tests that define their OWN 'test_db' fixture (e.g. test_support_case_service.py)
    skip this autouse fixture to avoid double-initialization.
    """
    if "test_db" in request.fixturenames:
        yield
        return

    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)
    db_path = Path(db_path)

    old_db_path = os.environ.get("SQLITE_DB_PATH")
    os.environ["SQLITE_DB_PATH"] = str(db_path)

    from src.db.init_db import init_database
    from src.common.db import db_session

    init_database(reset=True)

    # Seed minimal reference data for scenarios
    with db_session() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO clients (client_id, client_code, client_name, active_flag, created_at)
            VALUES
                (1, 'CASCADE', 'Cascade Health Alliance', 1, '2025-01-01T00:00:00'),
                (2, 'SUMMIT', 'Summit Benefits Group', 1, '2025-01-01T00:00:00')
        """)

        conn.execute("""
            INSERT OR IGNORE INTO vendors (vendor_id, vendor_code, vendor_name, vendor_type, active_flag, created_at)
            VALUES
                (1, 'MEDIPROC', 'MediProcess Solutions', 'TPA', 1, '2025-01-01T00:00:00'),
                (2, 'PHARMBR', 'PharmaBridge Inc', 'PBM', 1, '2025-01-01T00:00:00'),
                (3, 'CLEARPATH', 'ClearPath Data Services', 'CLEARINGHOUSE', 1, '2025-01-01T00:00:00'),
                (4, 'NATCLAIMS', 'National Claims Network', 'CLAIMS', 1, '2025-01-01T00:00:00')
        """)

        conn.execute("""
            INSERT OR IGNORE INTO benefit_plans (
                plan_id, plan_code, plan_name, plan_type, client_id, benefit_year,
                individual_deductible, family_deductible,
                individual_oop_max, family_oop_max,
                coinsurance_rate, active_flag
            )
            VALUES
                (1, 'PLN-001', 'Cascade Gold PPO', 'PPO', 1, 2025, 1500.00, 3000.00, 6000.00, 12000.00, 0.20, 1),
                (2, 'PLN-002', 'Cascade Silver HMO', 'HMO', 1, 2025, 3000.00, 6000.00, 8500.00, 17000.00, 0.20, 1),
                (3, 'PLN-003', 'Summit Standard PPO', 'PPO', 2, 2025, 2000.00, 4000.00, 7500.00, 15000.00, 0.20, 1)
        """)

        # Create minimal members for tests (reduced from 1100 to 10)
        members_data = []
        for i in range(10):  # Minimal set for testing
            member_id = f"MBR-BENCH-{i:03d}"
            subscriber_id = f"SUB-BENCH-{i//4:03d}"
            family_id = f"FAM-BENCH-{i//4:03d}"
            members_data.append((member_id, subscriber_id, 1, 'Bench', 'Member', '1980-01-01', 'M', 'SELF', family_id))

        # Add regular test members
        members_data.extend([
            ('MBR-001', 'SUB-001', 1, 'Robert', 'Johnson', '1978-03-15', 'M', 'SUB', 'FAM-001'),
            ('MBR-002', 'SUB-001', 1, 'Maria', 'Johnson', '1980-07-22', 'F', 'SPOUSE', 'FAM-001'),
            ('MBR-003', 'SUB-001', 1, 'Emma', 'Johnson', '2005-11-08', 'F', 'CHILD', 'FAM-001'),
            ('MBR-004', 'SUB-001', 1, 'Liam', 'Johnson', '2008-04-30', 'M', 'CHILD', 'FAM-001'),
            ('MBR-005', 'SUB-002', 1, 'David', 'Chen', '1982-01-10', 'M', 'SUB', 'FAM-002'),
            ('MBR-006', 'SUB-002', 1, 'Sarah', 'Chen', '1984-09-05', 'F', 'SPOUSE', 'FAM-002'),
            ('MBR-007', 'SUB-002', 1, 'Olivia', 'Chen', '2012-06-18', 'F', 'CHILD', 'FAM-002'),
            ('MBR-008', 'SUB-003', 2, 'James', 'Williams', '1975-12-01', 'M', 'SUB', 'FAM-003'),
            ('MBR-009', 'SUB-003', 2, 'Patricia', 'Williams', '1977-05-14', 'F', 'SPOUSE', 'FAM-003'),
            ('MBR-010', 'SUB-003', 2, 'Noah', 'Williams', '2010-08-25', 'M', 'CHILD', 'FAM-003'),
            ('MBR-011', 'SUB-004', 1, 'Emily', 'Davis', '1990-02-28', 'F', 'SUB', 'FAM-004'),
            ('MBR-012', 'SUB-005', 2, 'Michael', 'Brown', '1988-10-12', 'M', 'SUB', 'FAM-005'),
            ('MBR-099', 'SUB-099', 1, 'Test', 'Member', '1990-01-01', 'F', 'SUB', 'FAM-099')
        ])

        conn.executemany("""
            INSERT OR IGNORE INTO members (
                member_id, subscriber_id, client_id, first_name, last_name, dob, gender,
                relationship_code, family_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, members_data)

        conn.execute("""
            INSERT OR IGNORE INTO eligibility_periods (
                member_id, subscriber_id, client_id, plan_id, vendor_id,
                coverage_start, coverage_end, status
            )
            VALUES
                ('MBR-001', 'SUB-001', 1, 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-002', 'SUB-001', 1, 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-003', 'SUB-001', 1, 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-004', 'SUB-001', 1, 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-005', 'SUB-002', 1, 2, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-006', 'SUB-002', 1, 2, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-007', 'SUB-002', 1, 2, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-008', 'SUB-003', 2, 3, 3, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-009', 'SUB-003', 2, 3, 3, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-010', 'SUB-003', 2, 3, 3, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-011', 'SUB-004', 1, 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE'),
                ('MBR-012', 'SUB-005', 2, 3, 3, '2025-01-01', '2025-12-31', 'ACTIVE')
        """)

    yield db_path

    if old_db_path:
        os.environ["SQLITE_DB_PATH"] = old_db_path
    else:
        os.environ.pop("SQLITE_DB_PATH", None)

    db_path.unlink(missing_ok=True)


@pytest.fixture
def temp_db_path(isolated_test_db):
    return isolated_test_db


@pytest.fixture
def benchmark_db_path():
    """Provide a clean database path for performance benchmarks."""
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)
    db_path = Path(db_path)

    old_db_path = os.environ.get("SQLITE_DB_PATH")
    os.environ["SQLITE_DB_PATH"] = str(db_path)

    from src.db.init_db import init_database
    from src.common.db import db_session

    init_database(reset=True)

    # Add comprehensive benchmark reference data
    with db_session() as conn:
        # Clients and vendors
        conn.execute("""
            INSERT OR IGNORE INTO clients (client_id, client_code, client_name, active_flag, created_at)
            VALUES (1, 'BENCH', 'Benchmark Client', 1, '2025-01-01T00:00:00')
        """)

        conn.execute("""
            INSERT OR IGNORE INTO vendors (vendor_id, vendor_code, vendor_name, vendor_type, active_flag, created_at)
            VALUES (1, 'BENCH-VEN', 'Benchmark Vendor', 'TPA', 1, '2025-01-01T00:00:00')
        """)

        # Plans
        conn.execute("""
            INSERT OR IGNORE INTO benefit_plans (
                plan_id, plan_code, plan_name, plan_type, client_id, benefit_year,
                individual_deductible, family_deductible,
                individual_oop_max, family_oop_max,
                coinsurance_rate, active_flag
            )
            VALUES (1, 'BENCH-PLAN', 'Benchmark Plan', 'PPO', 1, 2025, 1000.00, 2000.00, 5000.00, 10000.00, 0.20, 1)
        """)

        # Create minimal members for benchmark tests (reduced from 1200 to 20)
        members_data = []
        for i in range(20):  # Minimal set for benchmark testing
            member_id = f"MBR-BENCH-{i:03d}"
            subscriber_id = f"SUB-BENCH-{i//4:03d}"
            family_id = f"FAM-BENCH-{i//4:03d}"
            members_data.append((member_id, subscriber_id, 1, 'Bench', 'Member', '1980-01-01', 'M', 'SELF', family_id))

        conn.executemany("""
            INSERT OR IGNORE INTO members (
                member_id, subscriber_id, client_id, first_name, last_name, dob, gender,
                relationship_code, family_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, members_data)

    yield str(db_path)

    if old_db_path:
        os.environ["SQLITE_DB_PATH"] = old_db_path
    else:
        os.environ.pop("SQLITE_DB_PATH", None)

    db_path.unlink(missing_ok=True)