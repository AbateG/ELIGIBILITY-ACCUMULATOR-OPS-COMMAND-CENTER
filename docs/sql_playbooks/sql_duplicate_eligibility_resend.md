# SQL Playbook: Duplicate Eligibility Resend

**Scenario Code:** `DUPLICATE_ELIGIBILITY_RESEND`

---

## Query 1: Identify Duplicate Files by Hash

```sql
SELECT file_hash, COUNT(*) AS file_count,
       GROUP_CONCAT(file_id) AS file_ids,
       GROUP_CONCAT(file_name) AS file_names,
       MIN(received_ts) AS first_received,
       MAX(received_ts) AS last_received
FROM inbound_files
WHERE file_type = 'eligibility'
GROUP BY file_hash
HAVING COUNT(*) > 1
ORDER BY last_received DESC;
```

**What it proves:** Identifies all eligibility files that share the same
hash, confirming identical content was received more than once.

**How to interpret:** Each group is a set of duplicates. The time gap between
`first_received` and `last_received` indicates whether it was an immediate
retry or a delayed re-send.

---

## Query 2: Processing Status of Duplicate Files

```sql
SELECT f.file_id, f.file_name, f.file_hash, f.received_ts,
       f.processing_status, f.duplicate_flag, f.row_count,
       r.run_id, r.run_status, r.rows_read, r.rows_passed, r.rows_failed
FROM inbound_files f
LEFT JOIN processing_runs r ON f.file_id = r.file_id
WHERE f.file_hash = '<DUPLICATE_HASH>'
ORDER BY f.received_ts;
```

**What it proves:** Shows whether each copy of the file was processed,
blocked, or is still pending.

**How to interpret:** The original should show `processing_status = 'SUCCESS'`.
The duplicate should ideally show `duplicate_flag = 1` and a non-processed
status. If both show SUCCESS, duplicate records may exist.

---

## Query 3: Check for Duplicate Eligibility Records

```sql
SELECT member_id, plan_id, coverage_start_date, coverage_end_date,
       COUNT(*) AS record_count,
       GROUP_CONCAT(DISTINCT file_id) AS source_files
FROM eligibility_records
WHERE file_id IN (
    SELECT file_id FROM inbound_files
    WHERE file_hash = '<DUPLICATE_HASH>'
)
GROUP BY member_id, plan_id, coverage_start_date, coverage_end_date
HAVING COUNT(*) > 1
ORDER BY record_count DESC;
```

**What it proves:** Identifies eligibility records that were loaded from
both the original and duplicate file.

**How to interpret:** Any row with `record_count > 1` is a confirmed
duplicate eligibility record that needs cleanup.

---

## Query 4: Accumulator Impact of Duplicates

```sql
SELECT t.member_id, t.accumulator_type,
       COUNT(*) AS transaction_count,
       SUM(t.transaction_amount) AS total_amount
FROM accumulator_transactions t
JOIN claim_records c ON t.claim_record_id = c.claim_record_id
JOIN eligibility_records e ON c.member_id = e.member_id
WHERE e.file_id IN (
    SELECT file_id FROM inbound_files
    WHERE file_hash = '<DUPLICATE_HASH>'
)
GROUP BY t.member_id, t.accumulator_type;
```

**What it proves:** Shows whether duplicate eligibility records led to
double-counted accumulator transactions.

**How to interpret:** Compare transaction counts against expected counts.
If counts are inflated, accumulators need rebuilding for affected members.

---

## Query 5: Issues and Cases for This Duplicate

```sql
SELECT dqi.issue_id, dqi.issue_code, dqi.severity, dqi.status,
       dqi.issue_description, dqi.detected_at,
       sc.case_id, sc.case_number, sc.assignment_group,
       sc.priority, sc.status AS case_status,
       sla.target_hours, sla.is_at_risk, sla.is_breached
FROM data_quality_issues dqi
LEFT JOIN support_cases sc ON dqi.issue_id = sc.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE dqi.issue_code = 'DUPLICATE_ELIGIBILITY_RESEND'
ORDER BY dqi.detected_at DESC;
```

**What it proves:** Full traceability from issue detection through case
routing and SLA status for this duplicate incident.

**How to interpret:** Case should be in `ops_eligibility_queue` with
24-hour SLA. Use this to track the resolution lifecycle.