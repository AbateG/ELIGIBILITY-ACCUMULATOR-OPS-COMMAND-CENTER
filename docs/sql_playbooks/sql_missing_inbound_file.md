# SQL Playbook: Missing Inbound File

**Scenario Code:** `MISSING_INBOUND_FILE`

These queries support root cause analysis for a missing inbound file incident.
Run them against the simulator's SQLite database (`sim.db`).

---

## Query 1: Files Expected Today That Haven't Arrived

```sql
SELECT file_id, file_name, file_type, client_id, vendor_id,
       expected_date, received_ts, processing_status
FROM inbound_files
WHERE expected_date = DATE('now')
  AND received_ts IS NULL
ORDER BY client_id, vendor_id;
```

**What it proves:** Identifies all files expected today with no arrival
timestamp. These are active gaps in the file delivery schedule.

**How to interpret:** Each row is a missing file. Cross-reference `client_id`
and `vendor_id` to determine which vendor relationship is affected. If
multiple files from the same vendor are missing, escalate as a vendor-wide
outage.

---

## Query 2: Vendor's Recent File Delivery History

```sql
SELECT file_id, file_name, file_type, expected_date, received_ts,
       processing_status, row_count
FROM inbound_files
WHERE vendor_id = '<VENDOR_ID>'
  AND expected_date >= DATE('now', '-7 days')
ORDER BY expected_date DESC, received_ts DESC;
```

**What it proves:** Shows whether this vendor has been reliably delivering
files over the past week. Establishes a pattern or isolates a one-time failure.

**How to interpret:** If recent files all arrived on time but today's is
missing, it's likely a one-time issue. If there's a pattern of late arrivals,
it may indicate a recurring vendor problem.

---

## Query 3: Issues Generated for Missing Files

```sql
SELECT dqi.issue_id, dqi.issue_code, dqi.severity, dqi.status,
       dqi.file_id, dqi.client_id, dqi.vendor_id,
       dqi.issue_description, dqi.detected_at
FROM data_quality_issues dqi
WHERE dqi.issue_code = 'MISSING_INBOUND_FILE'
ORDER BY dqi.detected_at DESC;
```

**What it proves:** Confirms the issue detection pipeline fired for this
missing file and shows the issue's current status.

**How to interpret:** There should be one issue per missing file. If no
issue exists for a known gap, the detection logic may need review.

---

## Query 4: Support Cases for Missing File Issues

```sql
SELECT sc.case_id, sc.case_number, sc.priority, sc.severity, sc.status,
       sc.assignment_group, sc.short_description,
       sc.opened_at, sc.resolved_at,
       sla.target_hours, sla.target_due_at, sla.is_at_risk, sla.is_breached
FROM support_cases sc
JOIN data_quality_issues dqi ON sc.issue_id = dqi.issue_id
LEFT JOIN sla_tracking sla ON sc.case_id = sla.case_id
WHERE dqi.issue_code = 'MISSING_INBOUND_FILE'
ORDER BY sc.opened_at DESC;
```

**What it proves:** Shows the full support case lifecycle — creation, routing,
SLA status — for missing file incidents.

**How to interpret:** `assignment_group` should be `ops_file_queue`. SLA
target should be 4 hours. If `is_at_risk = 1`, the case is approaching
breach. If `is_breached = 1`, the SLA was missed.

---

## Query 5: Impact Assessment — Members Affected

```sql
SELECT COUNT(DISTINCT e.member_id) AS affected_members
FROM eligibility_records e
JOIN inbound_files f ON e.file_id = f.file_id
WHERE f.vendor_id = '<VENDOR_ID>'
  AND f.client_id = '<CLIENT_ID>'
  AND f.expected_date = (
      SELECT MAX(expected_date) FROM inbound_files
      WHERE vendor_id = '<VENDOR_ID>'
        AND client_id = '<CLIENT_ID>'
        AND processing_status = 'SUCCESS'
  );
```

**What it proves:** Estimates how many members are covered by this vendor's
most recent successful file — these are the members at risk if today's file
is missing and contains updates.

**How to interpret:** A high member count increases urgency. Even if the
missing file is a delta, those members may have eligibility changes that
won't be reflected until the file arrives.