# Runbook: Accumulator Exceeds OOP Max

**Scenario Code:** `ACCUMULATOR_EXCEEDS_OOP_MAX`
**Severity:** HIGH
**SLA Target:** 8 hours
**Assignment Queue:** ops_recon_queue

---

## Symptom

A member's out-of-pocket accumulator balance exceeds the maximum allowed
by their benefit plan. The accumulator snapshot shows a total that is
greater than the plan's `oop_max` threshold.

## Impact

- Member may be overpaying for services (OOP max should cap member liability)
- If plan threshold is stale: plan data needs correction
- If accumulator is wrong: transactions need reversal or correction
- Financial and compliance risk in both directions
- HIGH priority with 8-hour SLA

## Triage Steps

### Step 1: Identify the Member and Current Accumulator State
From **Accumulator Reconciliation**, locate the flagged member:
```sql
SELECT s.snapshot_id, s.member_id, s.accumulator_type, s.period,
       s.total_amount, s.snapshot_date
FROM accumulator_snapshots s
WHERE s.member_id = '<MEMBER_ID>'
  AND s.accumulator_type = 'oop'
ORDER BY s.snapshot_date DESC
LIMIT 1;
```

### Step 2: Get the Plan's OOP Max
```sql
SELECT m.member_id, e.plan_id, p.plan_name, p.oop_max, p.deductible,
       p.plan_year_start, p.plan_year_end
FROM members m
JOIN eligibility_records e ON m.member_id = e.member_id
JOIN benefit_plans p ON e.plan_id = p.plan_id
WHERE m.member_id = '<MEMBER_ID>'
  AND e.status = 'active';
```

### Step 3: Calculate the Overage
```sql
SELECT s.total_amount AS current_oop,
       p.oop_max,
       (s.total_amount - p.oop_max) AS overage
FROM accumulator_snapshots s
JOIN eligibility_records e ON s.member_id = e.member_id
JOIN benefit_plans p ON e.plan_id = p.plan_id
WHERE s.member_id = '<MEMBER_ID>'
  AND s.accumulator_type = 'oop'
  AND e.status = 'active'
ORDER BY s.snapshot_date DESC
LIMIT 1;
```

### Step 4: Trace the Transactions
```sql
SELECT t.transaction_id, t.member_id, t.accumulator_type,
       t.transaction_amount, t.claim_record_id, t.transaction_date,
       t.created_at
FROM accumulator_transactions t
WHERE t.member_id = '<MEMBER_ID>'
  AND t.accumulator_type = 'oop'
ORDER BY t.transaction_date, t.created_at;
```

Look for:
- Duplicate transaction amounts from the same claim
- Unusually large single transactions
- Transactions that posted after a reversal should have applied

### Step 5: Check for Duplicate Claims
```sql
SELECT c.claim_record_id, c.member_id, c.date_of_service,
       c.billed_amount, c.allowed_amount, c.member_liability,
       c.claim_status
FROM claim_records c
WHERE c.member_id = '<MEMBER_ID>'
ORDER BY c.date_of_service;
```

## Likely Root Causes

| RCA Category | Description |
|---|---|
| Duplicate claim applied | Same claim posted to accumulators twice |
| Stale plan threshold | Plan's OOP max not updated for current benefit year |
| Reversal not reflected | A claim was reversed but accumulator wasn't decremented |
| Timing issue | Accumulator updated before plan data refresh |
| Incorrect member liability | Upstream claim processing calculated wrong member share |

## Remediation Path

1. If duplicate transaction: reverse the duplicate, rebuild snapshot.
2. If stale plan data: update the plan record, revalidate the breach.
3. If reversal missing: post the reversal transaction, rebuild snapshot.
4. If upstream claim error: flag for claims operations correction.
5. After correction: verify snapshot ≤ OOP max.

## Closure Criteria

- [ ] Overage amount quantified
- [ ] Root cause identified (duplicate, stale plan, reversal, etc.)
- [ ] Corrective transaction posted or plan data corrected
- [ ] Accumulator snapshot rebuilt and verified
- [ ] Member's accumulator total ≤ plan OOP max (or exception documented)
- [ ] Support case resolved with root cause documented