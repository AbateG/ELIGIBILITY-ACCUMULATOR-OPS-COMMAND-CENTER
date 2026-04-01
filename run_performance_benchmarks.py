#!/usr/bin/env python3
"""
Performance Benchmark Runner

This script runs comprehensive performance benchmarks for the Eligibility Accumulator
Operations Command Center and generates a detailed performance report.

Usage:
    python run_performance_benchmarks.py [--output FILE] [--verbose]

Options:
    --output FILE    Save results to JSON file
    --verbose        Show detailed output during execution
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Any
import argparse

# Import benchmark functions
from tests.test_performance_benchmarks import (
    create_benchmark_claims_file,
    create_benchmark_eligibility_file,
    setup_benchmark_database,
    PERFORMANCE_THRESHOLDS
)
from src.common.db import get_connection
from src.processing.process_eligibility import process_eligibility_files
from src.processing.process_claims import process_claim_files
from src.accumulators.snapshot_engine import rebuild_accumulator_snapshots, detect_accumulator_anomalies
from src.issues.support_case_service import create_support_cases_from_open_issues
from src.sla.sla_service import evaluate_open_slas


def run_claims_processing_benchmark(db_path: str, num_rows: int, verbose: bool = False) -> Dict[str, Any]:
    """Run claims processing benchmark."""
    if verbose:
        print(f"Running claims processing benchmark: {num_rows} rows")

    conn = get_connection(db_path)
    csv_file = None
    try:
        setup_benchmark_database(conn)

        # Create test file
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = create_benchmark_claims_file(num_rows, temp_dir)

            # Insert file record with unique ID
            file_id = 9000 + num_rows  # Unique per test
            conn.execute("""
                INSERT INTO inbound_files (file_id, file_name, file_type, landing_path, processing_status)
                VALUES (?, ?, ?, ?, ?)
            """, (file_id, f"benchmark_claims_{num_rows}.csv", "CLAIMS", csv_file, "VALIDATED"))
            conn.commit()

            # Measure performance
            start_time = time.time()
            process_claim_files(db_path=db_path)
            end_time = time.time()

            duration = end_time - start_time
            throughput = num_rows / duration if duration > 0 else 0

            threshold = PERFORMANCE_THRESHOLDS["claims_processing"].get(str(num_rows), {})
            max_duration = threshold.get('max_duration_seconds', float('inf'))
            passed = duration <= max_duration

            result = {
                "operation": "claims_processing",
                "scenario": "benchmark",
                "data_volume": num_rows,
                "duration_seconds": round(duration, 2),
                "throughput_per_second": round(throughput, 1),
                "threshold_seconds": max_duration if max_duration != float('inf') else None,
                "passed": passed,
                "timestamp": time.time()
            }

            if verbose:
                status = "PASS" if passed else "FAIL"
                print(f"  {status}: {duration:.2f}s ({throughput:.1f} rows/sec)")

            return result

    finally:
        conn.close()
        if csv_file:
            Path(csv_file).unlink(missing_ok=True)


def run_snapshot_rebuild_benchmark(db_path: str, num_transactions: int, verbose: bool = False) -> Dict[str, Any]:
    """Run snapshot rebuild benchmark."""
    if verbose:
        print(f"Running snapshot rebuild benchmark: {num_transactions} transactions")

    conn = get_connection(db_path)
    try:
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
                member_id, subscriber_id, 1, 1, i + 1000, 2025,
                "IND_DED" if i % 2 == 0 else "FAM_DED", 10.0, "2025-01-15", "CLAIM", 1000
            ))

        conn.commit()

        # Measure performance
        start_time = time.time()
        snapshots_rebuilt = rebuild_accumulator_snapshots(conn)
        conn.commit()
        end_time = time.time()

        duration = end_time - start_time

        threshold = PERFORMANCE_THRESHOLDS["snapshot_rebuild"].get(str(num_transactions), {})
        max_duration = threshold.get('max_duration_seconds', float('inf'))
        passed = duration <= max_duration

        result = {
            "operation": "snapshot_rebuild",
            "scenario": "benchmark",
            "data_volume": num_transactions,
            "duration_seconds": round(duration, 2),
            "throughput_per_second": round(num_transactions / duration, 1) if duration > 0 else 0,
            "snapshots_rebuilt": snapshots_rebuilt,
            "threshold_seconds": max_duration if max_duration != float('inf') else None,
            "passed": passed,
            "timestamp": time.time()
        }

        if verbose:
            status = "PASS" if passed else "FAIL"
            print(f"  {status}: {duration:.2f}s, rebuilt {snapshots_rebuilt} snapshots")

        return result

    finally:
        conn.close()


def run_anomaly_detection_benchmark(db_path: str, num_snapshots: int, verbose: bool = False) -> Dict[str, Any]:
    """Run anomaly detection benchmark."""
    if verbose:
        print(f"Running anomaly detection benchmark: {num_snapshots} snapshots")

    conn = get_connection(db_path)
    try:
        setup_benchmark_database(conn)

        # Create test snapshots
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
                float(i % 1000), float((i % 1000) * 2),
                float(i % 5000), float((i % 5000) * 2),
                0, 0, 0, 0
            ))

        conn.commit()

        # Measure performance
        start_time = time.time()
        anomalies_detected = detect_accumulator_anomalies(conn)
        conn.commit()
        end_time = time.time()

        duration = end_time - start_time

        threshold = PERFORMANCE_THRESHOLDS["anomaly_detection"].get(str(num_snapshots), {})
        max_duration = threshold.get('max_duration_seconds', float('inf'))
        passed = duration <= max_duration

        result = {
            "operation": "anomaly_detection",
            "scenario": "benchmark",
            "data_volume": num_snapshots,
            "duration_seconds": round(duration, 2),
            "throughput_per_second": round(num_snapshots / duration, 1) if duration > 0 else 0,
            "anomalies_detected": anomalies_detected,
            "threshold_seconds": max_duration if max_duration != float('inf') else None,
            "passed": passed,
            "timestamp": time.time()
        }

        if verbose:
            status = "PASS" if passed else "FAIL"
            print(f"  {status}: {duration:.2f}s, detected {anomalies_detected} anomalies")

        return result

    finally:
        conn.close()


def run_comprehensive_benchmarks(verbose: bool = False) -> List[Dict[str, Any]]:
    """Run all performance benchmarks."""
    import tempfile
    import os

    results = []

    # Create temporary database for benchmarks
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)

    try:
        # Initialize database
        os.environ['SQLITE_DB_PATH'] = db_path

        from src.db.init_db import init_database
        init_database(reset=True)

        if verbose:
            print("Running Performance Benchmarks")
            print("=" * 50)

        # Claims processing benchmarks
        if verbose:
            print("\nClaims Processing Benchmarks:")

        for num_rows in [100, 1000]:
            result = run_claims_processing_benchmark(db_path, num_rows, verbose)
            results.append(result)

        # Snapshot rebuild benchmarks
        if verbose:
            print("\nSnapshot Rebuild Benchmarks:")

        for num_txns in [1000, 10000]:
            result = run_snapshot_rebuild_benchmark(db_path, num_txns, verbose)
            results.append(result)

        # Anomaly detection benchmarks
        if verbose:
            print("\nAnomaly Detection Benchmarks:")

        for num_snapshots in [100, 1000]:
            result = run_anomaly_detection_benchmark(db_path, num_snapshots, verbose)
            results.append(result)

        if verbose:
            print("\nBenchmarking Complete!")

    finally:
        # Cleanup
        Path(db_path).unlink(missing_ok=True)
        if 'SQLITE_DB_PATH' in os.environ:
            del os.environ['SQLITE_DB_PATH']

    return results


def generate_performance_report(results: List[Dict[str, Any]]) -> str:
    """Generate a human-readable performance report."""
    report_lines = [
        "# Performance Benchmark Report",
        "",
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Summary",
        ""
    ]

    # Group results by operation
    by_operation = {}
    for result in results:
        op = result['operation']
        if op not in by_operation:
            by_operation[op] = []
        by_operation[op].append(result)

    total_passed = sum(1 for r in results if r['passed'])
    total_tests = len(results)

    report_lines.extend([
        f"- Total Benchmarks: {total_tests}",
        f"- Passed: {total_passed}",
        f"- Failed: {total_tests - total_passed}",
        ".1f"        "",
        "## Detailed Results",
        ""
    ])

    for operation, op_results in by_operation.items():
        report_lines.extend([
            f"### {operation.replace('_', ' ').title()}",
            "",
            "| Data Volume | Duration | Throughput | Threshold | Status |",
            "|-------------|----------|------------|-----------|--------|"
        ])

        for result in sorted(op_results, key=lambda x: x['data_volume']):
            status = "PASS" if result['passed'] else "FAIL"
            threshold = f"{result['threshold_seconds']}s" if result['threshold_seconds'] else "N/A"
            duration = f"{result['duration_seconds']}s"
            throughput = f"{result['throughput_per_second']}/s"

            report_lines.append(f"| {result['data_volume']} | {duration} | {throughput} | {threshold} | {status} |")

        report_lines.append("")

    return "\n".join(report_lines)


def main():
    parser = argparse.ArgumentParser(description="Run performance benchmarks")
    parser.add_argument("--output", "-o", help="Save results to JSON file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    parser.add_argument("--report", "-r", action="store_true", help="Generate and print performance report")

    args = parser.parse_args()

    # Run benchmarks
    results = run_comprehensive_benchmarks(verbose=args.verbose)

    # Save results if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")

    # Generate and print report if requested
    if args.report:
        report = generate_performance_report(results)
        print("\n" + "="*60)
        print(report)
        print("="*60)

    # Print summary
    total_passed = sum(1 for r in results if r['passed'])
    total_tests = len(results)
    print(f"\nBenchmark Summary: {total_passed}/{total_tests} passed")

    if total_passed < total_tests:
        print("❌ Some benchmarks failed - review thresholds or performance")
        return 1
    else:
        print("✅ All benchmarks passed!")
        return 0


if __name__ == "__main__":
    exit(main())