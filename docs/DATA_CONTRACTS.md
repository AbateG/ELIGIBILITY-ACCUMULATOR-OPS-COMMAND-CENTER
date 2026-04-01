# Data and Schema Contracts

This document formalizes the data contracts, schema relationships, and business rules for the Eligibility Accumulator Operations Command Center.

## Table of Contents
1. [Entity Lifecycles](#entity-lifecycles)
2. [Canonical Data Types and Constraints](#canonical-data-types-and-constraints)
3. [Foreign Key and Referential Integrity Rules](#foreign-key-and-referential-integrity-rules)
4. [Business Rules and Validation Logic](#business-rules-and-validation-logic)
5. [Processing Pipeline Contracts](#processing-pipeline-contracts)
6. [Error Handling and Recovery Contracts](#error-handling-and-recovery-contracts)

## Entity Lifecycles

### Inbound Files
Files follow a strict lifecycle through the processing pipeline:

| Status | Description | Transitions From | Transitions To |
|--------|-------------|------------------|----------------|
| `RECEIVED` | File ingested but not validated | N/A | `VALIDATED`, `FAILED` |
| `VALIDATED` | File passed schema and referential validation | `RECEIVED` | `PROCESSED`, `FAILED` |
| `PROCESSED` | File successfully processed (may have issues) | `VALIDATED` | N/A (terminal) |
| `FAILED` | File processing failed due to errors | `RECEIVED`, `VALIDATED` | N/A (terminal) |

**Invariants:**
- Files can only be processed once
- `FAILED` status is permanent and requires manual intervention
- Status transitions are monotonic (no backwards movement)

### Processing Runs
Runs track the execution of processing operations:

| Status | Description | Transitions From | Transitions To |
|--------|-------------|------------------|----------------|
| `RUNNING` | Processing currently executing | N/A | `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED` |
| `SUCCESS` | All rows processed without issues | `RUNNING` | N/A (terminal) |
| `PARTIAL_SUCCESS` | Some rows processed, some failed | `RUNNING` | N/A (terminal) |
| `FAILED` | Processing failed due to errors | `RUNNING` | N/A (terminal) |

**Invariants:**
- Each file gets exactly one run per processing type
- Run status reflects the most severe outcome encountered
- `FAILED` runs indicate systemic issues, not just data issues

### Issues
Issues represent data quality problems discovered during processing:

| Status | Description | Resolution Required |
|--------|-------------|---------------------|
| `OPEN` | Issue requires attention | Yes |
| `RESOLVED` | Issue has been addressed | No |

**Severity Levels:**
- `HIGH`: Critical data errors preventing processing
- `MEDIUM`: Significant data quality issues
- `LOW`: Minor inconsistencies or warnings

### Support Cases
Cases are created from unresolved issues requiring human intervention:

| Status | Description | Requires Action |
|--------|-------------|-----------------|
| `OPEN` | Case requires investigation | Yes |
| `IN_PROGRESS` | Case being actively worked | Yes |
| `RESOLVED` | Case has been resolved | No |
| `CLOSED` | Case permanently closed | No |

## Canonical Data Types and Constraints

### Member Data
```sql
member_id TEXT PRIMARY KEY        -- Format: MBR-{number}, unique across system
subscriber_id TEXT NOT NULL       -- Format: SUB-{number}
first_name TEXT NOT NULL
last_name TEXT NOT NULL
dob TEXT NOT NULL                 -- ISO format: YYYY-MM-DD
gender TEXT                       -- M, F, or NULL
relationship_code TEXT NOT NULL   -- SELF, SPOUSE, CHILD, etc.
family_id TEXT NOT NULL          -- Links family members
```

**Constraints:**
- `member_id` must be unique and follow naming convention
- `subscriber_id` groups family members under one subscriber
- `family_id` is typically the same as `subscriber_id` for the family head

### Plan Data
```sql
plan_id INTEGER PRIMARY KEY
plan_code TEXT NOT NULL UNIQUE    -- Format: PLN-{number}
benefit_year INTEGER NOT NULL     -- Calendar year for benefits
individual_deductible REAL NOT NULL
family_deductible REAL NOT NULL
individual_oop_max REAL NOT NULL
family_oop_max REAL NOT NULL
```

**Constraints:**
- All monetary values must be >= 0
- Family limits must be >= individual limits
- `benefit_year` determines accumulator reset periods

### Accumulator Types
Supported accumulator types with specific business rules:

| Type | Description | Reset Condition |
|------|-------------|-----------------|
| `IND_DED` | Individual Deductible | Per benefit year |
| `FAM_DED` | Family Deductible | Per benefit year |
| `IND_OOP` | Individual Out-of-Pocket Maximum | Per benefit year |
| `FAM_OOP` | Family Out-of-Pocket Maximum | Per benefit year |

## Foreign Key and Referential Integrity Rules

### Hard References (Required)
- `claims.member_id` → `members.member_id`
- `claims.plan_id` → `benefit_plans.plan_id`
- `eligibility_periods.member_id` → `members.member_id`
- `accumulator_transactions.claim_record_id` → `claims.claim_record_id`

### Soft References (Optional)
- `claims.vendor_id` → `vendors.vendor_id` (NULL allowed)
- `eligibility_periods.vendor_id` → `vendors.vendor_id` (NULL allowed)

**Cascade Behavior:**
- Deleting a member cascades to claims, eligibility, and accumulator data
- Deleting a plan prevents new claims but preserves historical data
- Vendor deletions are allowed (historical claims remain valid)

## Business Rules and Validation Logic

### Claim Processing Rules

#### Deductible Accumulation
- Deductibles accumulate until the individual/family limit is met
- Once met, `individual_deductible_met_flag` or `family_deductible_met_flag` is set
- Further deductible amounts are ignored for that member/family/year

#### Out-of-Pocket Maximum
- OOP accumulates after deductibles are met
- Includes coinsurance, copays, and member responsibility
- Once met, `individual_oop_met_flag` or `family_oop_met_flag` is set

#### Preventive Care
- Claims marked as preventive are exempt from deductible accumulation
- Determined by `preventive_flag` or procedure codes

#### Claim Reversals
- Negative amounts reduce accumulator totals
- Identified by `reversal_flag` or negative amounts
- Can reduce accumulators below zero (representing overpayments)

### Eligibility Validation Rules

#### Coverage Period Overlap
- Members cannot have overlapping coverage for the same plan
- Conflicting periods create HIGH severity issues

#### Coverage Gaps
- Gaps between coverage periods are allowed
- No validation for continuous coverage (business rule)

#### Retroactive Changes
- Eligibility changes can be retroactive
- Accumulators must be recalculated when eligibility changes

### Anomaly Detection Rules

#### Negative Accumulators
- Accumulators below zero trigger MEDIUM severity issues
- Indicates potential data errors or reversals

#### Family vs Individual Inconsistencies
- Family accumulator should be >= individual accumulator for same member
- Violations trigger HIGH severity issues

#### Missing Expected Accumulators
- Members with claims should have corresponding accumulator snapshots
- Missing snapshots trigger HIGH severity issues

## Processing Pipeline Contracts

### File Processing Order
1. **Eligibility files** must be processed before claims files for the same members
2. **Claims files** can be processed in any order within a benefit year
3. **Accumulator rebuilds** should follow all claim processing

### Transaction Boundaries
- Each file processes in a single database transaction
- Failure at any step rolls back the entire file's changes
- Successful processing commits all changes atomically

### Idempotency Guarantees
- Reprocessing the same file produces identical results
- Multiple processing attempts are safe (last write wins)
- Audit logs capture all processing attempts

### Error Propagation
- Validation errors prevent further processing of that row
- System errors (IO, database) fail the entire file
- Partial failures result in `PARTIAL_SUCCESS` status

## Error Handling and Recovery Contracts

### Failure Classification

#### Recoverable Failures
- Network timeouts (retry allowed)
- Temporary database unavailability
- File locking conflicts

#### Non-Recoverable Failures
- Invalid file format
- Schema violations
- Missing required data

#### Data Quality Issues
- Individual row problems
- Create issues but allow file completion
- Severity determines support case creation

### Recovery Procedures

#### File Reprocessing
1. Reset file status to `VALIDATED`
2. Clear previous processing results
3. Re-run processing pipeline
4. Preserve audit history

#### Manual Data Correction
1. Resolve underlying data issues
2. Update source systems
3. Reprocess affected files
4. Verify accumulator corrections

#### System Recovery
1. Restore from backup if needed
2. Replay failed operations
3. Validate data consistency
4. Resume normal processing

### Data Consistency Guarantees

#### Atomicity
- File processing is all-or-nothing
- Partial states are never persisted

#### Durability
- Committed changes survive system failures
- Failed operations leave no partial state

#### Isolation
- Concurrent file processing doesn't interfere
- Serial processing within member boundaries

This contract ensures predictable system behavior and enables reliable operations across all components of the Eligibility Accumulator system.