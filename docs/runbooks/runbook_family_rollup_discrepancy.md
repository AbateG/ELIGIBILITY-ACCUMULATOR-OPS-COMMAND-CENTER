# Runbook: Family Rollup Discrepancy

**Scenario Code:** `FAMILY_ROLLUP_DISCREPANCY`
**Severity:** MEDIUM
**SLA Target:** 24 hours
**Assignment Queue:** ops_recon_queue

---

## Symptom

The family-level accumulator total does not match the sum of individual
member-level accumulator transactions within the same family. A
reconciliation delta exists between the two levels.

## Impact

- Family deductible or OOP threshold may be evaluated against incorrect total
- Members within the family may hit (or fail to hit) family thresholds
  at the wrong time
- Could affect cost-sharing calculations for all family members
- MEDIUM priority — financial impact exists but is less immediate than
  OOP max breach

## Triage Steps

### Step 1: Identify the Family and the Delta
From **Accumulator Reconciliation**, locate the flagged family:
```sql
SELECT s.member_id, s.accumulator_type, s.total_amount, s.period,
       m.family_id
FROM accumulator_snapshots s
JOIN members m ON s.member_id = m.member_id
WHERE m.family_id = '<FAMILY_ID>'
ORDER BY s.accumulator_type, m.member_id;
```

### Step 2: Sum Individual Member Transactions
```sql
SELECT m.family_id, t.accumulator_type,
       SUM(t.transaction_amount) AS sum_member_transactions
FROM accumulator_transactions t
JOIN members m ON t.member_id = m.member_id
WHERE m.family_id = '<FAMILY_ID>'
GROUP BY m.family_id, t.accumulator_type;
```

### Step 3: Get Family-Level Snapshot
```sql
SELECT s.total_amount AS family_snapshot_total, s.accumulator_type
FROM accumulator_snapshots s
WHERE s.member_id = '<FAMILY_ID>'
  AND s.accumulator_type IN ('oop', 'deductible');
```
*(Note: family-level snapshots may use `family_id` as the entity key
depending on implementation.)*

### Step 4: Calculate the Discrepancy
```sql
SELECT fam.accumulator_type,
       fam.family_snapshot_total,
       ind.sum_member_transactions,
       (fam.family_snapshot_total - ind.sum_member_transactions) AS delta
FROM (
    SELECT s.accumulator_type, s.total_amount AS family_snapshot_total
    FROM accumulator_snapshots s
    WHERE s.member_id = '<FAMILY_ID>'
) fam
JOIN (
    SELECT t.accumulator_type,
           SUM(t.transaction_amount) AS sum_member_transactions
    FROM accumulator_transactions t
    JOIN members m ON t.member_id = m.member_id
    WHERE m.family_id = '<FAMILY_ID>'
    GROUP BY t.accumulator_type
) ind ON fam.accumulator_type = ind.accumulator_type;
```

### Step 5: Identify the Source of the Delta
```sql
SELECT t.transaction_id, t.member_id, t.accumulator_type,
       t.transaction_amount, t.claim_record_id, t.transaction_date,
       m.family_id
FROM accumulator_transactions t
JOIN members m ON t.member_id = m.member_id
WHERE m.family_id = '<FAMILY_ID>'
ORDER BY t.transaction_date, t.member_id;
```

Look for:
- Transactions posted to a member who was recently added/removed from family
- Transactions that exist at family level but not at member level (or vice versa)
- Timing gaps where member was in a different family grouping

## Likely Root Causes

| RCA Category | Description |
|---|---|
| Member added mid-year | New member joined family but prior individual accumulators not rolled up |
| Member removed mid-year | Member left family but their transactions still count in family total |
| Transaction level mismatch | Posted to individual but not rolled up to family (or vice versa) |
| Duplicate family adjustment | Manual adjustment applied at family level without member-level offset |
| Stale snapshot | Family snapshot not rebuilt after member-level correction |

## Remediation Path

1. Quantify the delta and determine which direction (family over or under).
2. Trace the specific transactions causing the discrepancy.
3. If member composition changed: adjust family total for current membership.
4. If rollup logic error: correct the rollup and rebuild snapshot.
5. If stale snapshot: rebuild family snapshot from current member transactions.
6. Verify family total = sum of current member totals after correction.

## Closure Criteria

- [ ] Delta quantified and direction identified
- [ ] Root cause of discrepancy identified
- [ ] Corrective action taken (transaction adjustment, snapshot rebuild, etc.)
- [ ] Family total verified = sum of member totals
- [ ] No downstream threshold evaluations impacted (or impact documented)
- [ ] Support case resolved with root cause documented