# Runbook: Missing Inbound File

**Scenario Code:** `MISSING_INBOUND_FILE`
**Severity:** CRITICAL
**SLA Target:** 4 hours
**Assignment Queue:** ops_file_queue

---

## Symptom

An expected eligibility file from a vendor has not been received by the
expected delivery time. The file monitoring system has no record of the
file arriving, or the `inbound_files` entry shows `received_ts` as NULL.

## Impact

- Members covered by this file may show as ineligible
- Claims arriving for these members will fail eligibility checks
- Downstream accumulator processing is blocked for affected members
- Member and provider call volume may increase
- SLA is 4 hours from detection — this is a CRITICAL priority

## Triage Steps

### Step 1: Confirm the Gap
Open **File Monitoring** in the Streamlit app. Look for the expected file
in the file inventory. Confirm that:
- The file is expected for today's date (check `expected_date`)
- No file with a matching pattern has arrived (`received_ts` is NULL)
- The file type and client/vendor match the expected delivery

### Step 2: Check for Partial or Renamed Files
Query `inbound_files` for the same client and vendor with today's date:
```sql
SELECT file_id, file_name, file_type, received_ts, processing_status
FROM inbound_files
WHERE client_id = '<CLIENT_ID>'
  AND vendor_id = '<VENDOR_ID>'
  AND expected_date = DATE('now')
ORDER BY received_ts DESC;
```
Look for files that may have arrived with an unexpected name.

### Step 3: Check Vendor's Other Files
Determine if the vendor's other scheduled files arrived:
```sql
SELECT file_id, file_name, file_type, expected_date, received_ts
FROM inbound_files
WHERE vendor_id = '<VENDOR_ID>'
  AND expected_date >= DATE('now', '-3 days')
ORDER BY expected_date DESC, received_ts DESC;
```
If all files from this vendor are missing, it suggests a vendor-wide issue.
If only this file is missing, it's file-specific.

### Step 4: Review the Support Case
Open **Issue Triage**. Locate the support case:
- `case_type` should reflect the file issue
- `assignment_group = 'ops_file_queue'`
- `priority = 'CRITICAL'`
- Check `is_at_risk` and `is_breached` on the SLA record

### Step 5: Check Recent Processing Runs
```sql
SELECT r.run_id, r.run_type, r.file_id, r.run_status, r.started_at
FROM processing_runs r
JOIN inbound_files f ON r.file_id = f.file_id
WHERE f.client_id = '<CLIENT_ID>'
  AND f.vendor_id = '<VENDOR_ID>'
ORDER BY r.started_at DESC
LIMIT 10;
```
Confirm no recent run attempted to process a file for this delivery.

## Likely Root Causes

| RCA Category | Description |
|---|---|
| Vendor SFTP failure | Vendor's file transfer process failed silently |
| Vendor generation error | Vendor's upstream system did not produce the file |
| Naming convention change | File arrived but with an unrecognized name |
| Network / transfer issue | File is in transit or was dropped during transfer |
| Schedule misalignment | Expected date is wrong in monitoring config |

## Remediation Path

1. If vendor-side: contact vendor operations team, request re-send or
   status update. Document the communication in the case notes.
2. If naming issue: locate the actual file, rename or remap the pattern,
   and re-trigger intake.
3. If transfer issue: check landing path, re-initiate transfer if possible.
4. Once file arrives: process through standard pipeline and verify
   downstream eligibility records load correctly.