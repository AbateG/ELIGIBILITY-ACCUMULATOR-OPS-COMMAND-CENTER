# Runbook: Claim for Ineligible Member

**Scenario Code:** `CLAIM_INELIGIBLE_MEMBER`
**Severity:** HIGH
**SLA Target:** 8 hours
**Assignment Queue:** ops_claims_queue

---

## Symptom

A claim has been received for a member who has no active eligibility
coverage as of the claim's date of service. The claim cannot be
processed through normal adjudication.

## Impact

- If eligibility is genuinely missing: claim must be held or denied
- If eligibility is delayed: claim should be held pending eligibility load
- If eligibility was retroactively terminated: claim may need reversal
- Member may be impacted at point of service
- HIGH priority with 8-hour SLA

## Triage Steps

### Step 1: Identify the Member and Claim
From **Issue Triage**, locate the case and extract:
- `member_id` from the linked issue
- `claim_record_id` from the linked issue
- Date of service from the claim

```sql
SELECT c.claim_record_id, c.member_id, c.date_of_service,
       c.billed_amount, c.claim_status
FROM claim_records c
WHERE c.claim_record_id = '<CLAIM_RECORD_ID>';
```

### Step 2: Check Eligibility for This Member
```sql
SELECT e.eligibility_id, e.member_id, e.plan_id, e.coverage_start_date,
       e.coverage_end_date, e.status
FROM eligibility_records e
WHERE e.member_id = '<MEMBER_ID>'
ORDER BY e.coverage_start_date DESC;
```

Look for:
- Any active record overlapping the date of service
- Recent terminations
- Gaps between coverage periods

### Step 3: Check for Pending Eligibility Files
```sql
SELECT f.file_id, f.file_name, f.file_type, f.processing_status,
       f.received_ts
FROM inbound_files f
WHERE f.file_type = 'eligibility'
  AND f.processing_status IN ('PENDING', 'RUNNING')
ORDER BY f.received_ts DESC;
```
If there's a pending eligibility file, the claim may resolve after processing.

### Step 4: Check Member's Plan
```sql
SELECT m.member_id, m.first_name, m.last_name, m.date_of_birth,
       m.client_id, m.family_id,
       p.plan_id, p.plan_name, p.plan_type
FROM members m
LEFT JOIN eligibility_records e ON m.member_id = e.member_id
LEFT JOIN benefit_plans p ON e.plan_id = p.plan_id
WHERE m.member_id = '<MEMBER_ID>';
```

### Step 5: Check if This is Part of a Broader Pattern
```sql
SELECT dqi.issue_code, COUNT(*) as issue_count
FROM data_quality_issues dqi
WHERE dqi.issue_code = 'CLAIM_INELIGIBLE_MEMBER'
  AND dqi.detected_at >= DATETIME('now', '-24 hours')
GROUP BY dqi.issue_code;
```
If multiple claims are hitting this, it may indicate a missing file rather
than a single-member issue.

## Likely Root Causes

| RCA Category | Description |
|---|---|
| Late eligibility file | File hasn't been processed yet; member will be eligible once loaded |
| Retroactive termination | Member was terminated effective before the claim date |
| Plan migration gap | Member moved plans and the new enrollment hasn't loaded |
| Data entry error | Member's eligibility dates are incorrect at source |
| Coverage gap | Legitimate gap in member's coverage |

## Remediation Path

1. If pending eligibility file: hold the claim, flag for reprocessing
   after eligibility loads.
2. If retroactive termination: confirm with client whether termination
   is correct. If error, request correction file.
3. If legitimate gap: claim may need to be denied; document the finding.
4. If widespread (multiple members): escalate to file-level investigation;
   check for MISSING_INBOUND_FILE.

## Closure Criteria

- [ ] Member's eligibility status confirmed as of date of service
- [ ] Claim disposition determined (hold, reprocess, deny)
- [ ] If eligibility error: correction requested from source
- [ ] If pattern: linked to file-level issue
- [ ] Support case resolved with root cause documented