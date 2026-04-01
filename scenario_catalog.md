# Scenario Catalog

This document describes every deterministic support scenario in the Eligibility
& Accumulator Operations Command Center. Each scenario represents a real
production incident pattern encountered in healthcare eligibility and
accumulator operations.

---

## How Scenarios Work

Each scenario is a **deterministic loader** that injects a specific data
condition into the simulator database, then triggers the standard validation
and issue-detection pipeline. The result is a fully traceable chain:

```
Scenario Trigger
  → Data Condition (file, eligibility, claim, or accumulator anomaly)
    → data_quality_issues record(s)
      → support_cases record(s) with deterministic routing
        → sla_tracking record(s) with scenario-specific targets
```

Scenarios are launched from the **Scenario Control Center** page and
investigated from the linked operational pages.

---

## Scenario Index

| # | Code | Issue Type | Severity | Queue | SLA (hrs) | Investigation Page |
|---|---|---|---|---|---|---|
| 1 | `MISSING_INBOUND_FILE` | file | CRITICAL | ops_file_queue | 4 | File Monitoring |
| 2 | `DUPLICATE_ELIGIBILITY_RESEND` | eligibility | MEDIUM | ops_eligibility_queue | 24 | File Monitoring |
| 3 | `CLAIM_INELIGIBLE_MEMBER` | claim | HIGH | ops_claims_queue | 8 | Issue Triage |
| 4 | `ACCUMULATOR_EXCEEDS_OOP_MAX` | accumulator | HIGH | ops_recon_queue | 8 | Accumulator Reconciliation |
| 5 | `FAMILY_ROLLUP_DISCREPANCY` | accumulator | MEDIUM | ops_recon_queue | 24 | Accumulator Reconciliation |

---

## Scenario 1: MISSING_INBOUND_FILE

### Business Meaning
An expected eligibility file from a vendor did not arrive by its scheduled
delivery window. This blocks downstream eligibility loading, which means
members may not have active coverage when claims are processed.

### Trigger
The scenario loader creates an `inbound_files` record with `received_ts` as
NULL or marks an expected delivery date with no matching file, then generates
the corresponding data quality issue.

### Generated Artifacts
- **data_quality_issues**: `issue_code = 'MISSING_INBOUND_FILE'`, `severity = 'CRITICAL'`
- **support_cases**: routed to `ops_file_queue`, `priority = 'CRITICAL'`
- **sla_tracking**: `target_hours = 4`

### Why It Matters
In production, a missing eligibility file can cause members to show as
ineligible at point of service. Pharmacies reject claims. Providers delay
procedures. Member calls spike. This is often the highest-urgency issue type
in eligibility operations.

### Investigation Approach
1. Check File Monitoring for the expected file and its delivery gap
2. Identify the client and vendor associated with the missing file
3. Check whether the vendor's other files arrived (isolate vendor-wide vs
   file-specific failure)
4. Review the support case in Issue Triage for SLA status
5. Likely RCA: vendor SFTP failure, file generation error, naming convention
   change, or network/transfer issue

### Related Documentation
- [Runbook](runbooks/runbook_missing_inbound_file.md)
- [SQL Playbook](sql_playbooks/sql_missing_inbound_file.md)

---

## Scenario 2: DUPLICATE_ELIGIBILITY_RESEND

### Business Meaning
A vendor resent an eligibility file that was already received and processed.
If loaded again without detection, it could create duplicate eligibility
records, causing downstream issues in claims processing and accumulator
calculations.

### Trigger
The scenario loader registers a second inbound file with a matching
`file_hash` to a previously processed file, and/or sets `duplicate_flag = 1`,
then generates the corresponding data quality issue.

### Generated Artifacts
- **data_quality_issues**: `issue_code = 'DUPLICATE_ELIGIBILITY_RESEND'`, `severity = 'MEDIUM'`
- **support_cases**: routed to `ops_eligibility_queue`, `priority = 'MEDIUM'`
- **sla_tracking**: `target_hours = 24`

### Why It Matters
Duplicate files are common in production. Vendors resend files after perceived
failures, or automated retry logic triggers a second transmission. Without
detection, double-loading creates ghost eligibility records, inflated member
counts, and potentially doubled accumulator credits.

### Investigation Approach
1. Check File Monitoring for duplicate indicators (`duplicate_flag`,
   matching `file_hash`)
2. Compare the original and duplicate file metadata (timestamps, row counts)
3. Verify whether the duplicate was blocked from processing or partially loaded
4. If loaded, check for duplicate eligibility records by member and coverage dates
5. Likely RCA: vendor retry, SFTP re-drop, missing idempotency check

### Related Documentation
- [Runbook](runbooks/runbook_duplicate_eligibility_resend.md)
- [SQL Playbook](sql_playbooks/sql_duplicate_eligibility_resend.md)

---

## Scenario 3: CLAIM_INELIGIBLE_MEMBER

### Business Meaning
A claim was submitted for a member who does not have active eligibility
coverage on the claim's service date. The claim cannot be adjudicated
properly and requires investigation to determine whether the eligibility
gap is legitimate or the result of a data issue.

### Trigger
The scenario loader creates a claim record referencing a member with no
overlapping active eligibility record for the claim's date of service,
then generates the corresponding data quality issue.

### Generated Artifacts
- **data_quality_issues**: `issue_code = 'CLAIM_INELIGIBLE_MEMBER'`, `severity = 'HIGH'`
- **support_cases**: routed to `ops_claims_queue`, `priority = 'HIGH'`
- **sla_tracking**: `target_hours = 8`

### Why It Matters
Claims for ineligible members are among the most frequent support issues.
They can result from late eligibility file loading, retroactive terminations,
member plan changes that haven't propagated, or genuine coverage gaps. Each
one requires careful investigation because incorrectly denying a valid claim
or paying an invalid one has direct financial and member-experience impact.

### Investigation Approach
1. Open Issue Triage and locate the case
2. Identify the member and claim service date
3. Query eligibility records for that member — look for coverage gaps,
   recent terminations, or pending eligibility loads
4. Check whether a recent eligibility file is still in processing
5. Likely RCA: late eligibility file, retroactive termination, member
   plan migration gap, or data entry error

### Related Documentation
- [Runbook](runbooks/runbook_claim_ineligible_member.md)
- [SQL Playbook](sql_playbooks/sql_claim_ineligible_member.md)

---

## Scenario 4: ACCUMULATOR_EXCEEDS_OOP_MAX

### Business Meaning
A member's tracked out-of-pocket accumulator balance exceeds the maximum
allowed by their benefit plan. This indicates either an overpayment, a
processing error, or a stale plan threshold — and the member may be owed
money or may be incorrectly blocked from further claims.

### Trigger
The scenario loader creates accumulator transactions that push the member's
OOP total past their plan's `oop_max` threshold, then generates the
corresponding data quality issue.

### Generated Artifacts
- **data_quality_issues**: `issue_code = 'ACCUMULATOR_EXCEEDS_OOP_MAX'`, `severity = 'HIGH'`
- **support_cases**: routed to `ops_recon_queue`, `priority = 'HIGH'`
- **sla_tracking**: `target_hours = 8`

### Why It Matters
OOP maximums are a member protection. Once a member hits their OOP max,
the plan should cover 100% of remaining costs. If the accumulator exceeds
this threshold without triggering the coverage change, the member is
overpaying. If the threshold data is wrong, the plan may be underpaying.
Either direction has financial and compliance implications.

### Investigation Approach
1. Open Accumulator Reconciliation and locate the flagged member
2. Review the member's accumulator snapshot vs their plan's OOP max
3. Trace the transaction history to identify which claim(s) pushed the
   total past the threshold
4. Verify the plan's OOP max value is current and correctly linked
5. Likely RCA: duplicate claim applied, plan threshold not updated for
   new benefit year, reversed claim not reflected, or timing issue
   between accumulator update and plan data refresh

### Related Documentation
- [Runbook](runbooks/runbook_accumulator_oop_exceeded.md)
- [SQL Playbook](sql_playbooks/sql_accumulator_oop_exceeded.md)

---

## Scenario 5: FAMILY_ROLLUP_DISCREPANCY

### Business Meaning
The family-level accumulator total does not match the sum of the individual
member-level accumulator transactions within that family. This discrepancy
indicates a data integrity issue in the family rollup logic.

### Trigger
The scenario loader creates a family group where the family accumulator
snapshot value diverges from the sum of member-level transactions, then
generates the corresponding data quality issue.

### Generated Artifacts
- **data_quality_issues**: `issue_code = 'FAMILY_ROLLUP_DISCREPANCY'`, `severity = 'MEDIUM'`
- **support_cases**: routed to `ops_recon_queue`, `priority = 'MEDIUM'`
- **sla_tracking**: `target_hours = 24`

### Why It Matters
Many benefit plans have both individual and family deductible/OOP thresholds.
The family total should equal the sum of all family members' individual
amounts. When these diverge, it can mean a transaction was applied to the
wrong level, a member was added or removed from the family mid-year without
adjustment, or the rollup calculation has a logic defect.

### Investigation Approach
1. Open Accumulator Reconciliation and locate the flagged family
2. Compare the family-level snapshot total to the sum of individual
   member snapshots
3. Identify the delta amount and direction (family over or under)
4. Trace individual member transactions and look for missing, duplicated,
   or misattributed entries
5. Likely RCA: member added/removed mid-year, transaction posted to
   individual but not rolled up, duplicate family-level adjustment,
   or stale snapshot not rebuilt after correction

### Related Documentation
- [Runbook](runbooks/runbook_family_rollup_discrepancy.md)
- [SQL Playbook](sql_playbooks/sql_family_rollup_discrepancy.md)

---

## Scenario Design Principles

1. **Deterministic** — Every scenario produces the same artifacts given the
   same starting state. No randomness in issue generation or routing.

2. **Traceable** — Every artifact links back: case → issue → file/member/claim
   → scenario trigger.

3. **Operationally realistic** — Each scenario mirrors a real production
   incident pattern encountered in healthcare eligibility and accumulator
   operations.

4. **Investigation-oriented** — Scenarios are designed to be investigated,
   not just observed. The value is in the triage and RCA process.

5. **SLA-governed** — Every scenario has a severity-appropriate SLA that
   creates urgency and prioritization context.