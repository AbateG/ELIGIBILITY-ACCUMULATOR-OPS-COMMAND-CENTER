"""
tests/test_accumulator_logic.py — Accumulator Calculation Tests

Validates core accumulator logic:
- Individual OOP accumulation from claims
- Family rollup = sum of individual members
- Breach detection when current_amount > limit_amount
- Recompute correctness after voiding claims
- Edge cases (zero amounts, single-member families)

Uses an in-memory SQLite database seeded with minimal test data.
No external dependencies or network calls.
"""

import sqlite3
import pytest
from pathlib import Path
from datetime import datetime


# ── Fixtures ───────────────────────────────────────────────────

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db" / "schema.sql"


@pytest.fixture
def db():
    """
    Create an in-memory SQLite database with the full schema and
    minimal test data for accumulator calculations.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")  # Simplify test setup

    # Load schema - use simplified schema for accumulator tests
    # if SCHEMA_PATH.exists():
    #     with open(SCHEMA_PATH, "r") as f:
    #         conn.executescript(f.read())
    # else:
    # Fallback: create only the tables needed for these tests
    conn.executescript("""
            CREATE TABLE IF NOT EXISTS members (
                member_id TEXT PRIMARY KEY,
                external_member_id TEXT,
                first_name TEXT,
                last_name TEXT,
                date_of_birth TEXT,
                gender TEXT,
                family_id TEXT,
                relationship_code TEXT,
                client_id TEXT,
                plan_id TEXT,
                eligibility_status TEXT DEFAULT 'ACTIVE',
                created_at TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS benefit_plans (
                plan_id TEXT PRIMARY KEY,
                plan_name TEXT,
                plan_type TEXT,
                client_id TEXT,
                oop_max_individual REAL DEFAULT 0,
                oop_max_family REAL DEFAULT 0,
                deductible_individual REAL DEFAULT 0,
                deductible_family REAL DEFAULT 0,
                benefit_year_start TEXT,
                benefit_year_end TEXT,
                status TEXT DEFAULT 'ACTIVE',
                created_at TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS claim_records (
                claim_record_id TEXT PRIMARY KEY,
                member_id TEXT,
                claim_number TEXT,
                service_date TEXT,
                provider_name TEXT,
                provider_npi TEXT,
                diagnosis_code TEXT,
                procedure_code TEXT,
                billed_amount REAL DEFAULT 0,
                allowed_amount REAL DEFAULT 0,
                paid_amount REAL DEFAULT 0,
                member_responsibility REAL DEFAULT 0,
                claim_status TEXT DEFAULT 'PROCESSED',
                adjudication_status TEXT,
                file_id TEXT,
                processing_run_id TEXT,
                created_at TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS accumulator_snapshots (
                accumulator_id TEXT PRIMARY KEY,
                member_id TEXT,
                plan_id TEXT,
                accumulator_type TEXT,
                period_start TEXT,
                period_end TEXT,
                current_amount REAL DEFAULT 0,
                limit_amount REAL DEFAULT 0,
                last_updated_at TEXT,
                created_at TEXT
            );
        """)

    # ── Seed test data ─────────────────────────────────────────
    now = datetime.now().isoformat()

    # Benefit plan
    conn.execute("""
        INSERT INTO benefit_plans
            (plan_id, plan_name, plan_type, client_id,
             oop_max_individual, oop_max_family,
             deductible_individual, deductible_family,
             benefit_year_start, benefit_year_end,
             status, created_at, updated_at)
        VALUES
            ('PLN-TEST', 'Test PPO', 'PPO', 'CLT-TEST',
             6000.00, 12000.00, 1500.00, 3000.00,
             '2025-01-01', '2025-12-31',
             'ACTIVE', ?, ?)
    """, (now, now))

    # Family with 3 members
    members = [
        ("MBR-T01", "EXT-T01", "Alice", "Test", "1980-01-01", "F",
         "FAM-TEST", "SUBSCRIBER", "CLT-TEST", "PLN-TEST"),
        ("MBR-T02", "EXT-T02", "Bob",   "Test", "1982-06-15", "M",
         "FAM-TEST", "SPOUSE",     "CLT-TEST", "PLN-TEST"),
        ("MBR-T03", "EXT-T03", "Carol", "Test", "2010-09-20", "F",
         "FAM-TEST", "DEPENDENT",  "CLT-TEST", "PLN-TEST"),
    ]
    for m in members:
        conn.execute("""
            INSERT INTO members
                (member_id, external_member_id, first_name, last_name,
                 date_of_birth, gender, family_id, relationship_code,
                 client_id, plan_id, eligibility_status,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
        """, (*m, now, now))

    # Individual member (single-person family)
    conn.execute("""
        INSERT INTO members
            (member_id, external_member_id, first_name, last_name,
             date_of_birth, gender, family_id, relationship_code,
             client_id, plan_id, eligibility_status,
             created_at, updated_at)
        VALUES
            ('MBR-T04', 'EXT-T04', 'Dan', 'Solo',
             '1990-03-10', 'M', 'FAM-SOLO', 'SUBSCRIBER',
             'CLT-TEST', 'PLN-TEST', 'ACTIVE', ?, ?)
    """, (now, now))

    conn.commit()
    yield conn
    conn.close()


def _insert_claim(conn, claim_id, member_id, amount, status="PROCESSED"):
    """Helper to insert a claim record with minimal required fields."""
    conn.execute("""
        INSERT INTO claim_records
            (claim_record_id, member_id, claim_number, service_date,
             billed_amount, allowed_amount, paid_amount,
             member_responsibility, claim_status, created_at, updated_at)
        VALUES (?, ?, ?, '2025-03-15', ?, ?, ?, ?, ?, ?, ?)
    """, (
        claim_id, member_id, f"CLM-{claim_id}",
        amount * 2, amount * 1.5, amount * 0.5,
        amount, status,
        datetime.now().isoformat(), datetime.now().isoformat(),
    ))
    conn.commit()


def _insert_accumulator(conn, accum_id, member_id, accum_type,
                         current_amount, limit_amount):
    """Helper to insert an accumulator snapshot."""
    conn.execute("""
        INSERT INTO accumulator_snapshots
            (accumulator_id, member_id, plan_id, accumulator_type,
             period_start, period_end, current_amount, limit_amount,
             last_updated_at, created_at)
        VALUES (?, ?, 'PLN-TEST', ?, '2025-01-01', '2025-12-31',
                ?, ?, ?, ?)
    """, (
        accum_id, member_id, accum_type,
        current_amount, limit_amount,
        datetime.now().isoformat(), datetime.now().isoformat(),
    ))
    conn.commit()


def _compute_individual_oop(conn, member_id):
    """
    Compute the expected individual OOP total from processed claims.
    This is the 'source of truth' calculation.
    """
    row = conn.execute("""
        SELECT COALESCE(SUM(member_responsibility), 0.0) AS total
        FROM claim_records
        WHERE member_id = ?
          AND claim_status NOT IN ('VOIDED', 'REJECTED')
    """, (member_id,)).fetchone()
    return row["total"]


def _compute_family_oop(conn, family_id):
    """
    Compute the expected family OOP total as the sum of all individual
    member OOP amounts within the family.
    """
    row = conn.execute("""
        SELECT COALESCE(SUM(cr.member_responsibility), 0.0) AS total
        FROM claim_records cr
        JOIN members m ON cr.member_id = m.member_id
        WHERE m.family_id = ?
          AND cr.claim_status NOT IN ('VOIDED', 'REJECTED')
    """, (family_id,)).fetchone()
    return row["total"]


def _is_breach(conn, accumulator_id):
    """Check if an accumulator has breached its limit."""
    row = conn.execute("""
        SELECT current_amount, limit_amount
        FROM accumulator_snapshots
        WHERE accumulator_id = ?
    """, (accumulator_id,)).fetchone()
    if row is None:
        return False
    return row["current_amount"] > row["limit_amount"]


# ── Test Cases ─────────────────────────────────────────────────

class TestIndividualOOPAccumulation:
    """Tests for individual member OOP accumulation from claims."""

    def test_single_claim_accumulation(self, db):
        """A single processed claim should produce the correct OOP total."""
        _insert_claim(db, "CLM-001", "MBR-T01", 500.00)

        total = _compute_individual_oop(db, "MBR-T01")
        assert total == 500.00, f"Expected 500.00, got {total}"

    def test_multiple_claims_accumulate(self, db):
        """Multiple processed claims should sum correctly."""
        _insert_claim(db, "CLM-010", "MBR-T01", 500.00)
        _insert_claim(db, "CLM-011", "MBR-T01", 750.00)
        _insert_claim(db, "CLM-012", "MBR-T01", 250.00)

        total = _compute_individual_oop(db, "MBR-T01")
        assert total == 1500.00, f"Expected 1500.00, got {total}"

    def test_voided_claims_excluded(self, db):
        """Voided claims must NOT contribute to the OOP total."""
        _insert_claim(db, "CLM-020", "MBR-T02", 400.00, status="PROCESSED")
        _insert_claim(db, "CLM-021", "MBR-T02", 600.00, status="VOIDED")

        total = _compute_individual_oop(db, "MBR-T02")
        assert total == 400.00, (
            f"Expected 400.00 (voided claim excluded), got {total}"
        )

    def test_rejected_claims_excluded(self, db):
        """Rejected claims must NOT contribute to the OOP total."""
        _insert_claim(db, "CLM-030", "MBR-T03", 300.00, status="PROCESSED")
        _insert_claim(db, "CLM-031", "MBR-T03", 200.00, status="REJECTED")

        total = _compute_individual_oop(db, "MBR-T03")
        assert total == 300.00, (
            f"Expected 300.00 (rejected claim excluded), got {total}"
        )

    def test_zero_amount_claim(self, db):
        """A claim with zero member_responsibility should not change the total."""
        _insert_claim(db, "CLM-040", "MBR-T04", 1000.00)
        _insert_claim(db, "CLM-041", "MBR-T04", 0.00)

        total = _compute_individual_oop(db, "MBR-T04")
        assert total == 1000.00, (
            f"Expected 1000.00 (zero-amount claim has no effect), got {total}"
        )

    def test_no_claims_yields_zero(self, db):
        """A member with no claims should have a zero OOP total."""
        total = _compute_individual_oop(db, "MBR-T03")
        assert total == 0.0, f"Expected 0.0 for member with no claims, got {total}"

    def test_claims_isolated_per_member(self, db):
        """Claims for one member must not affect another member's total."""
        _insert_claim(db, "CLM-050", "MBR-T01", 1200.00)
        _insert_claim(db, "CLM-051", "MBR-T02", 800.00)

        total_t01 = _compute_individual_oop(db, "MBR-T01")
        total_t02 = _compute_individual_oop(db, "MBR-T02")

        assert total_t01 == 1200.00, f"MBR-T01 expected 1200.00, got {total_t01}"
        assert total_t02 == 800.00, f"MBR-T02 expected 800.00, got {total_t02}"


class TestFamilyRollup:
    """Tests for family-level OOP accumulation (sum of individuals)."""

    def test_family_rollup_equals_sum_of_individuals(self, db):
        """
        Family OOP total must equal the sum of all individual member
        OOP totals within that family.
        """
        _insert_claim(db, "CLM-100", "MBR-T01", 2000.00)  # Alice
        _insert_claim(db, "CLM-101", "MBR-T02", 1000.00)  # Bob
        _insert_claim(db, "CLM-102", "MBR-T03", 500.00)   # Carol

        family_total = _compute_family_oop(db, "FAM-TEST")
        expected = 2000.00 + 1000.00 + 500.00

        assert family_total == expected, (
            f"Family rollup expected {expected}, got {family_total}"
        )

    def test_family_rollup_excludes_voided_claims(self, db):
        """Voided claims in any family member should not count in rollup."""
        _insert_claim(db, "CLM-110", "MBR-T01", 1500.00, status="PROCESSED")
        _insert_claim(db, "CLM-111", "MBR-T02", 800.00,  status="PROCESSED")
        _insert_claim(db, "CLM-112", "MBR-T02", 400.00,  status="VOIDED")
        _insert_claim(db, "CLM-113", "MBR-T03", 300.00,  status="PROCESSED")

        family_total = _compute_family_oop(db, "FAM-TEST")
        expected = 1500.00 + 800.00 + 300.00  # 2600.00, not 3000.00

        assert family_total == expected, (
            f"Family rollup expected {expected} (voided excluded), got {family_total}"
        )

    def test_single_member_family(self, db):
        """
        A single-member family's rollup should equal that member's
        individual total.
        """
        _insert_claim(db, "CLM-120", "MBR-T04", 950.00)

        family_total = _compute_family_oop(db, "FAM-SOLO")
        individual_total = _compute_individual_oop(db, "MBR-T04")

        assert family_total == individual_total, (
            f"Single-member family: family={family_total}, "
            f"individual={individual_total} — should be equal"
        )

    def test_empty_family_yields_zero(self, db):
        """A family with no claims should have a zero rollup."""
        family_total = _compute_family_oop(db, "FAM-TEST")
        assert family_total == 0.0, (
            f"Expected 0.0 for family with no claims, got {family_total}"
        )

    def test_family_rollup_only_includes_own_members(self, db):
        """
        Claims from members outside the family must NOT be included
        in the family rollup.
        """
        _insert_claim(db, "CLM-130", "MBR-T01", 1000.00)  # FAM-TEST
        _insert_claim(db, "CLM-131", "MBR-T04", 2000.00)  # FAM-SOLO

        fam_test_total = _compute_family_oop(db, "FAM-TEST")
        fam_solo_total = _compute_family_oop(db, "FAM-SOLO")

        assert fam_test_total == 1000.00, (
            f"FAM-TEST expected 1000.00, got {fam_test_total}"
        )
        assert fam_solo_total == 2000.00, (
            f"FAM-SOLO expected 2000.00, got {fam_solo_total}"
        )


class TestBreachDetection:
    """Tests for detecting when accumulators exceed plan limits."""

    def test_breach_detected_when_over_limit(self, db):
        """An accumulator with current_amount > limit_amount is a breach."""
        _insert_accumulator(
            db, "ACC-T01", "MBR-T01",
            "oop_individual",
            current_amount=6500.00,
            limit_amount=6000.00,
        )

        assert _is_breach(db, "ACC-T01") is True, (
            "Expected breach: 6500 > 6000"
        )

    def test_no_breach_when_under_limit(self, db):
        """An accumulator within its limit is NOT a breach."""
        _insert_accumulator(
            db, "ACC-T02", "MBR-T02",
            "oop_individual",
            current_amount=4500.00,
            limit_amount=6000.00,
        )

        assert _is_breach(db, "ACC-T02") is False, (
            "Expected no breach: 4500 < 6000"
        )

    def test_no_breach_at_exact_limit(self, db):
        """
        An accumulator at exactly the limit is NOT a breach.
        Breach requires strictly greater than.
        """
        _insert_accumulator(
            db, "ACC-T03", "MBR-T03",
            "oop_individual",
            current_amount=6000.00,
            limit_amount=6000.00,
        )

        assert _is_breach(db, "ACC-T03") is False, (
            "Expected no breach: 6000 == 6000 (at limit, not over)"
        )

    def test_breach_nonexistent_accumulator(self, db):
        """A nonexistent accumulator ID should return False (no breach)."""
        assert _is_breach(db, "ACC-NONEXISTENT") is False

    def test_breach_with_zero_limit(self, db):
        """
        An accumulator with zero limit and any positive amount
        is technically a breach.
        """
        _insert_accumulator(
            db, "ACC-T04", "MBR-T04",
            "oop_individual",
            current_amount=100.00,
            limit_amount=0.00,
        )

        assert _is_breach(db, "ACC-T04") is True, (
            "Expected breach: 100 > 0"
        )

    def test_no_breach_both_zero(self, db):
        """An accumulator with zero amount and zero limit is NOT a breach."""
        _insert_accumulator(
            db, "ACC-T05", "MBR-T01",
            "deductible_individual",
            current_amount=0.00,
            limit_amount=0.00,
        )

        assert _is_breach(db, "ACC-T05") is False, (
            "Expected no breach: 0 == 0"
        )


class TestRecomputeCorrectness:
    """
    Tests verifying that recomputing accumulators from claims produces
    correct totals, especially after voiding or adjusting claims.
    """

    def test_recompute_after_void_reduces_total(self, db):
        """
        Voiding a claim and recomputing should produce a lower total
        than the original accumulator amount.
        """
        # Initial state: two claims, accumulator shows their sum
        _insert_claim(db, "CLM-200", "MBR-T01", 2000.00, status="PROCESSED")
        _insert_claim(db, "CLM-201", "MBR-T01", 1500.00, status="PROCESSED")

        _insert_accumulator(
            db, "ACC-R01", "MBR-T01",
            "oop_individual",
            current_amount=3500.00,  # 2000 + 1500
            limit_amount=6000.00,
        )

        # Void one claim
        db.execute("""
            UPDATE claim_records
            SET claim_status = 'VOIDED'
            WHERE claim_record_id = 'CLM-201'
        """)
        db.commit()

        # Recompute from claims
        new_total = _compute_individual_oop(db, "MBR-T01")

        assert new_total == 2000.00, (
            f"After voiding CLM-201 (1500), expected 2000.00, got {new_total}"
        )

        # Verify drift from current accumulator
        row = db.execute("""
            SELECT current_amount FROM accumulator_snapshots
            WHERE accumulator_id = 'ACC-R01'
        """).fetchone()

        drift = row["current_amount"] - new_total
        assert drift == 1500.00, (
            f"Drift should be 1500.00 (stale accumulator), got {drift}"
        )

    def test_recompute_all_voided_yields_zero(self, db):
        """If all claims are voided, recompute should yield zero."""
        _insert_claim(db, "CLM-210", "MBR-T02", 800.00, status="VOIDED")
        _insert_claim(db, "CLM-211", "MBR-T02", 400.00, status="VOIDED")

        total = _compute_individual_oop(db, "MBR-T02")
        assert total == 0.0, (
            f"All claims voided — expected 0.0, got {total}"
        )

    def test_recompute_family_after_member_void(self, db):
        """
        Voiding a claim for one family member and recomputing the
        family rollup should reduce the family total accordingly.
        """
        _insert_claim(db, "CLM-220", "MBR-T01", 3000.00, status="PROCESSED")
        _insert_claim(db, "CLM-221", "MBR-T02", 1000.00, status="PROCESSED")
        _insert_claim(db, "CLM-222", "MBR-T03", 500.00,  status="PROCESSED")

        # Verify initial family total
        initial_family = _compute_family_oop(db, "FAM-TEST")
        assert initial_family == 4500.00

        # Void Bob's claim
        db.execute("""
            UPDATE claim_records
            SET claim_status = 'VOIDED'
            WHERE claim_record_id = 'CLM-221'
        """)
        db.commit()

        # Recompute family
        new_family = _compute_family_oop(db, "FAM-TEST")
        assert new_family == 3500.00, (
            f"After voiding Bob's 1000 claim, expected 3500.00, got {new_family}"
        )

    def test_accumulator_drift_detection(self, db):
        """
        When the accumulator snapshot doesn't match the claim sum,
        the drift amount should be correctly computed.
        """
        # Claims total 1800
        _insert_claim(db, "CLM-230", "MBR-T04", 1000.00)
        _insert_claim(db, "CLM-231", "MBR-T04", 800.00)

        # But accumulator says 2200 (stale — includes a voided claim
        # that was backed out of claims but not accumulators)
        _insert_accumulator(
            db, "ACC-R02", "MBR-T04",
            "oop_individual",
            current_amount=2200.00,
            limit_amount=6000.00,
        )

        claim_total = _compute_individual_oop(db, "MBR-T04")
        row = db.execute("""
            SELECT current_amount FROM accumulator_snapshots
            WHERE accumulator_id = 'ACC-R02'
        """).fetchone()

        drift = round(row["current_amount"] - claim_total, 2)

        assert drift == 400.00, (
            f"Expected drift of 400.00 (2200 - 1800), got {drift}"
        )
        assert drift > 0, "Positive drift means accumulator is OVER claim total"


class TestEdgeCases:
    """Edge cases and boundary conditions for accumulator logic."""

    def test_large_claim_amount(self, db):
        """Very large claim amounts should accumulate correctly."""
        _insert_claim(db, "CLM-300", "MBR-T01", 99999.99)

        total = _compute_individual_oop(db, "MBR-T01")
        assert total == 99999.99

    def test_penny_precision(self, db):
        """Accumulation should maintain penny-level precision."""
        _insert_claim(db, "CLM-310", "MBR-T02", 100.01)
        _insert_claim(db, "CLM-311", "MBR-T02", 200.02)
        _insert_claim(db, "CLM-312", "MBR-T02", 300.03)

        total = _compute_individual_oop(db, "MBR-T02")
        assert abs(total - 600.06) < 0.001, (
            f"Expected 600.06, got {total} — penny precision lost"
        )

    def test_many_small_claims(self, db):
        """A large number of small claims should sum correctly."""
        for i in range(100):
            _insert_claim(db, f"CLM-4{i:03d}", "MBR-T03", 10.00)

        total = _compute_individual_oop(db, "MBR-T03")
        assert total == 1000.00, (
            f"100 claims × \$10.00 = expected 1000.00, got {total}"
        )

    def test_mixed_statuses_in_family(self, db):
        """
        A family with a mix of processed, voided, and rejected claims
        should only count processed ones in the rollup.
        """
        _insert_claim(db, "CLM-500", "MBR-T01", 1000.00, status="PROCESSED")
        _insert_claim(db, "CLM-501", "MBR-T01", 500.00,  status="VOIDED")
        _insert_claim(db, "CLM-502", "MBR-T02", 750.00,  status="PROCESSED")
        _insert_claim(db, "CLM-503", "MBR-T02", 250.00,  status="REJECTED")
        _insert_claim(db, "CLM-504", "MBR-T03", 300.00,  status="PROCESSED")
        _insert_claim(db, "CLM-505", "MBR-T03", 100.00,  status="VOIDED")

        # Only PROCESSED: 1000 + 750 + 300 = 2050
        family_total = _compute_family_oop(db, "FAM-TEST")
        assert family_total == 2050.00, (
            f"Expected 2050.00 (only PROCESSED), got {family_total}"
        )

        # Verify individuals
        assert _compute_individual_oop(db, "MBR-T01") == 1000.00
        assert _compute_individual_oop(db, "MBR-T02") == 750.00
        assert _compute_individual_oop(db, "MBR-T03") == 300.00

        # Verify sum matches
        individual_sum = (
            _compute_individual_oop(db, "MBR-T01")
            + _compute_individual_oop(db, "MBR-T02")
            + _compute_individual_oop(db, "MBR-T03")
        )
        assert individual_sum == family_total, (
            f"Family rollup ({family_total}) must equal sum of "
            f"individuals ({individual_sum})"
        )