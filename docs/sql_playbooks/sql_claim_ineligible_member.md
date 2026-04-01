# SQL Playbook: Claim for Ineligible Member

**Scenario Code:** `CLAIM_INELIGIBLE_MEMBER`

---

## Query 1: The Claim and Its Member

```sql
SELECT c.claim_record_id, c.member_id, c.date_of_service,
       c.billed_amount, c.allowed_amount, c.member_liability,
       c.claim_status, c.file_id,
       m.first_name, m.last_name, m.date_of_birth, m.client_id
FROM claim_records c
JOIN members m ON c.member_id = m.member_id
WHERE c.claim_record_id = '<CLAIM_RECORD_ID>';
```

**What it proves:** Shows the claim details and the member it references.

**How to interpret:** Note the `date_of_service` — this is the date that
must be covered by an active eligibility record.

---

## Query 2: Member's Full Eligibility History

```sql
SELECT e.eligibility_id, e.member_id, e.plan_id, e.coverage_start_date,
       e.coverage_end_date, e.status, e.file_id,
       p.plan_name, p.plan_type
FROM eligibility_records e
LEFT JOIN benefit_plans p ON e.plan_id = p.plan_id
WHERE e.member_id = '<MEMBER_ID>'
ORDER BY e.coverage_start_date DESC;
```

**What it proves:** Full coverage timeline for the member. Look for gaps
around the date of service.

**How to interpret:** If no row has `coverage_start_date <= date_of_service
<= coverage_end_date` with `status = 'active'`, the member is genuinely
ineligible as of that date. Look for recent terminations or pending records.

---

## Query 3: Coverage Gap Analysis

```sql
SELECT e1.coverage_end_date AS prior_end,
       e2.coverage_start_date AS next_start,
       JULIANDAY(e2.coverage_start_date) - JULIANDAY(e1.coverage_end_date)
         AS gap_days
FROM eligibility_records e1
JOIN eligibility_records e2
  ON e1.member_id = e2.member_id
  AND e1.coverage_end_date < e2.coverage_start_date
WHERE e1.member_id = '<MEMBER_ID>'
ORDER BY e1.coverage_end_date;
```

**What it proves:** Identifies explicit gaps between coverage periods.

**How to interpret:** A gap that overlaps the claim's date of service
confirms the eligibility issue. The gap size helps determine if it's a
data lag or a true coverage break.

---

## Query 4: Pending Eligibility Files That Might Resolve This

```sql
SELECT f.file_id, f.file_name, f.file_type, f.client_id, f.vendor_id,
       f.received_ts, f.processing_status, f.row_count
FROM inbound_files f
WHERE f.file_type = 'eligibility'
  AND f.client_id = (SELECT client_id FROM members WHERE member_id = '<MEMBER_ID>')
  AND f.processing_status IN ('PENDING', 'RUNNING')
ORDER BY f.received_ts DESC;
```

**What it proves:** Shows whether there's an unprocessed eligibility file
that could contain this member's coverage record.

**How to interpret:** If a pending file exists, the ineligibility may be
temporary. Hold the claim rather than denying.

---

## Query 5: Pattern Check — Multiple Ineligible Claims Today

```sql
SELECT dqi.member_id, dqi.claim_record_id, dqi.severity, dqi.status,
       dqi.detected_at, dqi.client_id
FROM data_quality_issues dqi
WHERE dqi.issue_code = 'CLAIM_INELIGIBLE_MEMBER'
  AND DATE(dqi.detected_at) = DATE('now')
ORDER BY dqi.client_id, dqi.detected_at;
```

**What it proves:** Determines whether this is an isolated case or part of
a pattern (e.g., multiple members from the same client).

**How to interpret:** Multiple claims for different members from the same
`client_id` suggest a missing eligibility file, not a single-member issue.
Escalate accordingly.

---

## Query 6: Support Case and SLA Status

```sql
SELECT sc.case_id, sc.case_number, sc.priority, sc.status,
       sc.assignment_group, sc.short_description,
       sc.opened_at, sc.acknowledged_at, sc.resolved_at,
       sc.root_cause_category,
       sla.target_hours, sla.target_due_at, sla.is_at_risk, sla.is_breached
FROM support_cases sc
JOIN data_quality_issues dqi ON sc.issue_id = dqi.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE dqi.issue_code = 'CLAIM_INELIGIBLE_MEMBER'
  AND dqi.claim_record_id = '<CLAIM_RECORD_ID>';
```

**What it proves:** Full case lifecycle for this specific claim's ineligibility
issue.

**How to interpret:** Should route to `ops_claims_queue` with 8-hour SLA.
Track acknowledgment and resolution timestamps against the SLA target.