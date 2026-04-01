# Runbook: Duplicate Eligibility Resend

**Scenario Code:** `DUPLICATE_ELIGIBILITY_RESEND`
**Severity:** MEDIUM
**SLA Target:** 24 hours
**Assignment Queue:** ops_eligibility_queue

---

## Symptom

An eligibility file has been received that matches a previously processed
file. Detection may be via `file_hash` match, `duplicate_flag = 1`, or
identical file content with a different filename/timestamp.

## Impact

- If loaded without detection: duplicate eligibility records, inflated
  member counts, potential double-counted accumulators
- If blocked: no immediate data impact, but requires confirmation that
  the duplicate was intentional or accidental
- Lower urgency than a missing file, but must be resolved to maintain
  data integrity

## Triage Steps

### Step 1: Confirm the Duplicate
Open **File Monitoring**. Locate the flagged file and its predecessor:
```sql
SELECT file_id, file_name, file_hash, received_ts, processing_status,
       duplicate_flag, row_count
FROM inbound_files
WHERE file_hash = '<DUPLICATE_HASH>'
ORDER BY received_ts;
```

### Step 2: Compare File Metadata
Verify:
- Do both files have the same `file_hash`?
- Do they have the same `row_count`?
- What are the `received_ts` values? How far apart?
- Was the original file successfully processed?

### Step 3: Check Processing Status of Both Files
```sql
SELECT f.file_id, f.file_name, f.processing_status, f.duplicate_flag,
       r.run_id, r.run_status, r.rows_read, r.rows_passed
FROM inbound_files f
LEFT JOIN processing_runs r ON f.file_id = r.file_id
WHERE f.file_hash = '<DUPLICATE_HASH>'
ORDER BY f.received_ts;
```

### Step 4: Check for Duplicate Eligibility Records
If the duplicate was processed, look for doubled records:
```sql
SELECT member_id, plan_id, coverage_start_date, coverage_end_date,
       COUNT(*) as record_count
FROM eligibility_records
WHERE file_id IN ('<ORIGINAL_FILE_ID>', '<DUPLICATE_FILE_ID>')
GROUP BY member_id, plan_id, coverage_start_date, coverage_end_date
HAVING COUNT(*) > 1;
```

### Step 5: Review the Support Case
Locate the case in **Issue Triage**:
- `assignment_group = 'ops_eligibility_queue'`
- `priority = 'MEDIUM'`

## Likely Root Causes

| RCA Category | Description |
|---|---|
| Vendor retry | Vendor's automated retry resent after a perceived failure |
| Manual re-drop | Vendor operator manually re-sent the file |
| SFTP re-transmission | Network issue caused a duplicate transfer |
| Missing idempotency | Pipeline did not check for duplicate before processing |

## Remediation Path

1. If duplicate was blocked: confirm the original was fully processed.
   Close the case as expected behavior with no data impact.
2. If duplicate was partially or fully loaded: identify and remove
   the duplicate eligibility records. Re-run accumulator snapshots
   for affected members.
3. Communicate with vendor if the resend was unintentional.
4. Document whether the duplicate detection worked or needs improvement.

## Closure Criteria

- [ ] Duplicate file identified and classified (blocked vs loaded)
- [ ] If loaded: duplicate eligibility records cleaned
- [ ] If loaded: affected accumulator snapshots rebuilt
- [ ] No downstream claims impacted by duplicate records
- [ ] Support case resolved with root cause documented
- [ ] Vendor notified if resend was accidental