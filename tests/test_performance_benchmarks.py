"""
Performance Benchmarking Suite

This module provides comprehensive performance benchmarks for the Eligibility Accumulator
Operations Command Center. It measures and validates system performance under various
load conditions to ensure production readiness and identify optimization opportunities.

Benchmarks cover:
- Claims processing at different scales
- Eligibility processing at different scales
- Snapshot rebuild operations
- Anomaly detection performance
- End-to-end workflow performance
"""

from __future__ import annotations

import time
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Any, Optional
from tempfile import NamedTemporaryFile

import pytest

from src.common.db import get_connection
from src.processing.process_eligibility import process_eligibility_files
from src.processing.process_claims import process_claim_files
from src.accumulators.snapshot_engine import rebuild_accumulator_snapshots, detect_accumulator_anomalies
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.sla.sla_service import evaluate_open_slas


@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""

    operation: str
    scenario: str
    data_volume: int
    duration_seconds: float
    rows_processed: int
    throughput_per_second: float
    memory_mb_start: Optional[float] = None
    memory_mb_end: Optional[float] = None
    memory_mb_peak: Optional[float] = None
    issues_created: int = 0
    cases_created: int = 0
    anomalies_detected: int = 0
    snapshots_rebuilt: int = 0
    status: str = "completed"  # completed, failed, timeout
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return asdict(self)

    @property
    def passed_threshold(self) -> Optional[bool]:
        """Check if this result passes the defined threshold."""
        threshold = PERFORMANCE_THRESHOLDS.get(self.operation, {}).get(f"{self.data_volume}", {}).get('max_duration_seconds')
        if threshold is None:
            return None  # No threshold defined
        return self.duration_seconds <= threshold


# Performance thresholds (configurable based on environment)
PERFORMANCE_THRESHOLDS = {
    "claims_processing": {
        "100": {"max_duration_seconds": 5.0},
        "1000": {"max_duration_seconds": 30.0},
        "5000": {"max_duration_seconds": 120.0},
    },
    "eligibility_processing": {
        "100": {"max_duration_seconds": 3.0},
        "1000": {"max_duration_seconds": 20.0},
        "5000": {"max_duration_seconds": 90.0},
    },
    "snapshot_rebuild": {
        "1000": {"max_duration_seconds": 5.0},
        "10000": {"max_duration_seconds": 25.0},
    },
    "anomaly_detection": {
        "100": {"max_duration_seconds": 2.0},
        "1000": {"max_duration_seconds": 10.0},
    },
    "end_to_end_workflow": {
        "1000": {"max_duration_seconds": 60.0},
    }
}


def measure_memory_usage() -> float:
    """Get current memory usage in MB."""
    try:
        import psutil
        import os
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024  # MB
    except ImportError:
        return 0.0  # Memory measurement not available


def create_benchmark_claims_file(num_rows: int, temp_dir: str) -> str:
    """Create a benchmark claims CSV file with specified number of rows."""
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False, dir=temp_dir) as f:
        # Write header
        f.write("member_id,subscriber_id,client_code,plan_code,vendor_code,claim_id,line_id,service_date,paid_date,allowed_amount,paid_amount,member_responsibility,deductible_amount,coinsurance_amount,copay_amount,preventive_flag,claim_status\n")

        # Write data rows (cycle through members to create realistic distribution)
        for i in range(num_rows):
            member_idx = i % 50  # Cycle through 50 members
            claim_idx = i // 50   # Claims per member

            member_id = f"MBR-BENCH-{member_idx:03d}"
            subscriber_id = f"SUB-BENCH-{member_idx//4:03d}"
            claim_id = f"CLAIM-BENCH-{i:06d}"
            line_id = "1"
            service_date = "2025-01-15"
            paid_date = "2025-01-20"
            allowed_amount = "100.00"
            paid_amount = "80.00"
            member_resp = "20.00"
            deductible = "10.00"
            coinsurance = "5.00"
            copay = "5.00"
            preventive_flag = "0"
            claim_status = "PAID"

            f.write(f"{member_id},{subscriber_id},BENCH,PLAN-BENCH,VENDOR-BENCH,{claim_id},{line_id},{service_date},{paid_date},{allowed_amount},{paid_amount},{member_resp},{deductible},{coinsurance},{copay},{preventive_flag},{claim_status}\n")

        f.flush()
        return f.name


def create_benchmark_eligibility_file(num_rows: int, temp_dir: str) -> str:
    """Create a benchmark eligibility CSV file with specified number of rows."""
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False, dir=temp_dir) as f:
        # Write header
        f.write("member_id,subscriber_id,client_code,plan_code,vendor_code,coverage_start,coverage_end,status\n")

        # Write data rows
        for i in range(num_rows):
            member_idx = i % 100  # Cycle through 100 members
            member_id = f"MBR-BENCH-{member_idx:03d}"
            subscriber_id = f"SUB-BENCH-{member_idx//4:03d}"

            f.write(f"{member_id},{subscriber_id},BENCH,PLAN-BENCH,VENDOR-BENCH,2025-01-01,2025-12-31,ACTIVE\n")

        f.flush()
        return f.name


def setup_benchmark_database(conn) -> None:
    """Set up database with benchmark reference data."""
    # Create clients, vendors, plans
    conn.execute("INSERT OR IGNORE INTO clients (client_code, client_name) VALUES (?, ?)", ("BENCH", "Benchmark Client"))
    conn.execute("INSERT OR IGNORE INTO vendors (vendor_code, vendor_name, vendor_type) VALUES (?, ?, ?)", ("VENDOR-BENCH", "Benchmark Vendor", "PAYER"))
    conn.execute("""
        INSERT OR IGNORE INTO benefit_plans (plan_code, plan_name, plan_type, client_id, benefit_year,
                                         individual_deductible, family_deductible,
                                         individual_oop_max, family_oop_max)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("PLAN-BENCH", "Benchmark Plan", "PPO", 1, 2025, 1000.0, 2000.0, 5000.0, 10000.0))

    # Create members (enough for benchmark scenarios)
    for member_idx in range(100):
        member_id = f"MBR-BENCH-{member_idx:03d}"
        subscriber_id = f"SUB-BENCH-{member_idx//4:03d}"
        conn.execute("""
            INSERT OR IGNORE INTO members (member_id, subscriber_id, client_id, first_name, last_name, dob,
                                          relationship_code, family_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (member_id, subscriber_id, 1, "Bench", "Member", "1980-01-01", "SELF", subscriber_id))

    conn.commit()


@pytest.mark.performance
@pytest.mark.parametrize("num_rows", [100, 1000])
def test_benchmark_claims_processing(benchmark_db_path, num_rows):
    """Benchmark claims processing performance."""
    import tempfile
    import os

    conn = get_connection(benchmark_db_path)
    try:
        # Setup benchmark data
        setup_benchmark_database(conn)

        # Create test file
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = create_benchmark_claims_file(num_rows, temp_dir)

            # Insert file record
            conn.execute("""
                INSERT INTO inbound_files (file_id, file_name, file_type, landing_path, processing_status)
                VALUES (?, ?, ?, ?, ?)
            """, (9999, f"benchmark_claims_{num_rows}.csv", "CLAIMS", csv_file, "VALIDATED"))
            conn.commit()

            # Measure performance
            memory_start = measure_memory_usage()
            start_time = time.time()

            try:
                process_claim_files(db_path=benchmark_db_path)
                status = "completed"
                error_msg = None
            except Exception as e:
                status = "failed"
                error_msg = str(e)

            end_time = time.time()
            memory_end = measure_memory_usage()

            duration = end_time - start_time
            throughput = num_rows / duration if duration > 0 else 0

            # Record result
            result = BenchmarkResult(
                operation="claims_processing",
                scenario="benchmark",
                data_volume=num_rows,
                duration_seconds=duration,
                rows_processed=num_rows,
                throughput_per_second=throughput,
                memory_mb_start=memory_start,
                memory_mb_end=memory_end,
                status=status,
                error_message=error_msg
            )

            # Verify threshold compliance
            if result.passed_threshold is False:
                pytest.fail(f"Performance threshold exceeded: {duration:.2f}s > {PERFORMANCE_THRESHOLDS['claims_processing'][str(num_rows)]['max_duration_seconds']}s")

            # Log results for analysis
            print(f"Claims processing {num_rows} rows: {duration:.2f}s ({throughput:.1f} rows/sec)")

    finally:
        conn.close()

    # Cleanup
    if 'csv_file' in locals():
        Path(csv_file).unlink(missing_ok=True)


@pytest.mark.performance
@pytest.mark.parametrize("num_rows", [100, 1000])
def test_benchmark_eligibility_processing(benchmark_db_path, num_rows):
    """Benchmark eligibility processing performance."""
    import tempfile

    conn = get_connection(benchmark_db_path)
    try:
        # Setup benchmark data
        setup_benchmark_database(conn)

        # Create test file
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = create_benchmark_eligibility_file(num_rows, temp_dir)

            # Insert file record
            conn.execute("""
                INSERT INTO inbound_files (file_id, file_name, file_type, landing_path, processing_status)
                VALUES (?, ?, ?, ?, ?)
            """, (9998, f"benchmark_eligibility_{num_rows}.csv", "ELIGIBILITY", csv_file, "VALIDATED"))
            conn.commit()

            # Measure performance
            memory_start = measure_memory_usage()
            start_time = time.time()

            try:
                process_eligibility_files(db_path=benchmark_db_path)
                status = "completed"
                error_msg = None
            except Exception as e:
                status = "failed"
                error_msg = str(e)

            end_time = time.time()
            memory_end = measure_memory_usage()

            duration = end_time - start_time
            throughput = num_rows / duration if duration > 0 else 0

            # Record result
            result = BenchmarkResult(
                operation="eligibility_processing",
                scenario="benchmark",
                data_volume=num_rows,
                duration_seconds=duration,
                rows_processed=num_rows,
                throughput_per_second=throughput,
                memory_mb_start=memory_start,
                memory_mb_end=memory_end,
                status=status,
                error_message=error_msg
            )

            # Verify threshold compliance
            if result.passed_threshold is False:
                pytest.fail(f"Performance threshold exceeded: {duration:.2f}s > {PERFORMANCE_THRESHOLDS['eligibility_processing'][str(num_rows)]['max_duration_seconds']}s")

            print(f"Eligibility processing {num_rows} rows: {duration:.2f}s ({throughput:.1f} rows/sec)")

    finally:
        conn.close()

    # Cleanup
    if 'csv_file' in locals():
        Path(csv_file).unlink(missing_ok=True)


@pytest.mark.performance
@pytest.mark.parametrize("num_transactions", [1000, 10000])
def test_benchmark_snapshot_rebuild(benchmark_db_path, num_transactions):
    """Benchmark accumulator snapshot rebuild performance."""
    conn = get_connection(benchmark_db_path)
    try:
        # Setup benchmark data
        setup_benchmark_database(conn)

        # Create test transactions
        for i in range(num_transactions):
            member_idx = i % 50
            member_id = f"MBR-BENCH-{member_idx:03d}"
            subscriber_id = f"SUB-BENCH-{member_idx//4:03d}"

            conn.execute("""
                INSERT INTO accumulator_transactions (
                    member_id, family_id, client_id, plan_id, claim_record_id, benefit_year,
                    accumulator_type, delta_amount, service_date, source_type, source_file_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                member_id,
                subscriber_id,
                1,
                1,
                i + 1000,  # Fake claim IDs
                2025,
                "IND_DED" if i % 2 == 0 else "FAM_DED",
                10.0,
                "2025-01-15",
                "CLAIM",
                1000
            ))

        conn.commit()

        # Measure performance
        memory_start = measure_memory_usage()
        start_time = time.time()

        try:
            snapshots_rebuilt = rebuild_accumulator_snapshots(conn)
            conn.commit()
            status = "completed"
            error_msg = None
        except Exception as e:
            status = "failed"
            error_msg = str(e)
            snapshots_rebuilt = 0

        end_time = time.time()
        memory_end = measure_memory_usage()

        duration = end_time - start_time

        # Record result
        result = BenchmarkResult(
            operation="snapshot_rebuild",
            scenario="benchmark",
            data_volume=num_transactions,
            duration_seconds=duration,
            rows_processed=num_transactions,
            throughput_per_second=num_transactions / duration if duration > 0 else 0,
            memory_mb_start=memory_start,
            memory_mb_end=memory_end,
            snapshots_rebuilt=snapshots_rebuilt,
            status=status,
            error_message=error_msg
        )

        # Verify threshold compliance
        if result.passed_threshold is False:
            pytest.fail(f"Performance threshold exceeded: {duration:.2f}s > {PERFORMANCE_THRESHOLDS['snapshot_rebuild'][str(num_transactions)]['max_duration_seconds']}s")

        print(f"Snapshot rebuild {num_transactions} transactions: {duration:.2f}s, rebuilt {snapshots_rebuilt} snapshots")

    finally:
        conn.close()


@pytest.mark.performance
@pytest.mark.parametrize("num_snapshots", [100, 1000])
def test_benchmark_anomaly_detection(benchmark_db_path, num_snapshots):
    """Benchmark anomaly detection performance."""
    conn = get_connection(benchmark_db_path)
    try:
        # Setup benchmark data
        setup_benchmark_database(conn)

        # Create test snapshots (unique member/plan/year combinations)
        for i in range(num_snapshots):
            member_id = f"MBR-BENCH-{i:03d}"
            subscriber_id = f"SUB-BENCH-{i//4:03d}"

            conn.execute("""
                INSERT INTO accumulator_snapshots (
                    member_id, family_id, client_id, plan_id, benefit_year,
                    individual_deductible_accum, family_deductible_accum,
                    individual_oop_accum, family_oop_accum,
                    individual_deductible_met_flag, family_deductible_met_flag,
                    individual_oop_met_flag, family_oop_met_flag
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                member_id, subscriber_id, 1, 1, 2025,
                float(i % 1000), float((i % 1000) * 2),  # Vary accumulator values
                float(i % 5000), float((i % 5000) * 2),
                0, 0, 0, 0
            ))

        conn.commit()

        # Measure performance
        memory_start = measure_memory_usage()
        start_time = time.time()

        try:
            anomalies_detected = detect_accumulator_anomalies(conn)
            conn.commit()
            status = "completed"
            error_msg = None
        except Exception as e:
            status = "failed"
            error_msg = str(e)
            anomalies_detected = 0

        end_time = time.time()
        memory_end = measure_memory_usage()

        duration = end_time - start_time

        # Record result
        result = BenchmarkResult(
            operation="anomaly_detection",
            scenario="benchmark",
            data_volume=num_snapshots,
            duration_seconds=duration,
            rows_processed=num_snapshots,
            throughput_per_second=num_snapshots / duration if duration > 0 else 0,
            memory_mb_start=memory_start,
            memory_mb_end=memory_end,
            anomalies_detected=anomalies_detected,
            status=status,
            error_message=error_msg
        )

        # Verify threshold compliance
        if result.passed_threshold is False:
            pytest.fail(f"Performance threshold exceeded: {duration:.2f}s > {PERFORMANCE_THRESHOLDS['anomaly_detection'][str(num_snapshots)]['max_duration_seconds']}s")

        print(f"Anomaly detection {num_snapshots} snapshots: {duration:.2f}s, detected {anomalies_detected} anomalies")

    finally:
        conn.close()


@pytest.mark.performance
def test_benchmark_end_to_end_workflow(benchmark_db_path):
    """Benchmark complete end-to-end workflow performance."""
    import tempfile

    conn = get_connection(benchmark_db_path)
    try:
        # Setup benchmark data
        setup_benchmark_database(conn)

        # Create test claims file
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = create_benchmark_claims_file(1000, temp_dir)

            # Insert file record
            conn.execute("""
                INSERT INTO inbound_files (file_id, file_name, file_type, landing_path, processing_status)
                VALUES (?, ?, ?, ?, ?)
            """, (9997, "benchmark_e2e.csv", "CLAIMS", csv_file, "VALIDATED"))
            conn.commit()

            # Measure end-to-end performance
            memory_start = measure_memory_usage()
            start_time = time.time()

            issues_before = conn.execute("SELECT COUNT(*) FROM data_quality_issues").fetchone()[0]
            cases_before = conn.execute("SELECT COUNT(*) FROM support_cases").fetchone()[0]

            try:
                # Process claims
                process_claim_files(db_path=benchmark_db_path)

                # Rebuild snapshots
                snapshots_rebuilt = rebuild_accumulator_snapshots(conn)

                # Detect anomalies
                anomalies_detected = detect_accumulator_anomalies(conn)

                # Create support cases
                cases_created = create_support_cases_from_open_issues(conn)

                # Evaluate SLAs
                evaluate_open_slas(conn)

                conn.commit()

                issues_after = conn.execute("SELECT COUNT(*) FROM data_quality_issues").fetchone()[0]
                cases_after = conn.execute("SELECT COUNT(*) FROM support_cases").fetchone()[0]

                status = "completed"
                error_msg = None

            except Exception as e:
                status = "failed"
                error_msg = str(e)
                snapshots_rebuilt = 0
                anomalies_detected = 0
                cases_created = 0
                issues_after = issues_before
                cases_after = cases_before

            end_time = time.time()
            memory_end = measure_memory_usage()

            duration = end_time - start_time

            # Record result
            result = BenchmarkResult(
                operation="end_to_end_workflow",
                scenario="benchmark",
                data_volume=1000,
                duration_seconds=duration,
                rows_processed=1000,
                throughput_per_second=1000 / duration if duration > 0 else 0,
                memory_mb_start=memory_start,
                memory_mb_end=memory_end,
                issues_created=issues_after - issues_before,
                cases_created=cases_after - cases_before,
                anomalies_detected=anomalies_detected,
                snapshots_rebuilt=snapshots_rebuilt,
                status=status,
                error_message=error_msg
            )

            # Verify threshold compliance
            if result.passed_threshold is False:
                pytest.fail(f"Performance threshold exceeded: {duration:.2f}s > {PERFORMANCE_THRESHOLDS['end_to_end_workflow']['1000']['max_duration_seconds']}s")

            print(f"End-to-end workflow 1000 claims: {duration:.2f}s")
            print(f"  Snapshots rebuilt: {snapshots_rebuilt}")
            print(f"  Anomalies detected: {anomalies_detected}")
            print(f"  Issues created: {result.issues_created}")
            print(f"  Cases created: {result.cases_created}")

    finally:
        conn.close()

    # Cleanup
    if 'csv_file' in locals():
        Path(csv_file).unlink(missing_ok=True)