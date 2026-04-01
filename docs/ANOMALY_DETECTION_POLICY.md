# Anomaly Detection Policy

## Overview
This document defines the operational policy for accumulator anomaly detection, including severity levels, auto-case creation rules, and business rationale for each anomaly type.

## Policy Framework

### Severity Levels
- **CRITICAL**: System integrity compromised, requires immediate operational intervention
- **HIGH**: Significant data quality issue, requires prompt attention
- **MEDIUM**: Notable data quality concern, requires review but not urgent
- **LOW**: Minor inconsistency, monitoring/tracking only

### Auto-Case Creation Rules
- **CRITICAL**: Always create support case
- **HIGH**: Create support case for operationally actionable issues
- **MEDIUM**: Create issue only (no auto-case), dashboard visible
- **LOW**: Log only, no issue creation

## Anomaly Type Definitions

### NEGATIVE_ACCUMULATOR
**Subtype**: NEGATIVE_ACCUMULATOR

**Current Configuration**:
- Severity: HIGH
- Auto-case: Yes (via HIGH severity)

**Proposed Configuration**:
- Severity: MEDIUM
- Auto-case: No

**Business Rationale**:
- Negative accumulators can occur during valid business flows (reversals, corrections, adjustments)
- Not always indicative of system corruption or data quality issues
- Still requires review but not urgent operational intervention
- Reduces noise in support case queues while maintaining oversight

**Known Valid Scenarios**:
- Claim reversals/adjustments
- Retroactive eligibility changes
- Correction of overpayments
- System reconciliation adjustments

**Invalid Scenarios** (would remain concerning):
- Impossible negative values from invalid calculations
- Systemic data corruption

### IND_OOP_EXCEEDS_MAX
**Subtype**: IND_OOP_EXCEEDS_MAX

**Configuration**:
- Severity: CRITICAL
- Auto-case: Yes

**Business Rationale**:
- Strong indicator of system corruption or invalid business logic
- Exceeding maximum OOP limits is financially significant
- Requires immediate operational intervention
- Prevents potential overpayments or incorrect benefit calculations

### FAM_OOP_EXCEEDS_MAX
**Subtype**: FAM_OOP_EXCEEDS_MAX

**Configuration**:
- Severity: CRITICAL
- Auto-case: Yes

**Business Rationale**:
- Same critical nature as individual OOP exceedance
- Affects family benefit calculations
- Requires immediate review to prevent systemic errors

### FAMILY_ROLLUP_MISMATCH
**Subtype**: FAMILY_ROLLUP_MISMATCH

**Current Configuration**:
- Severity: HIGH
- Auto-case: Yes (via HIGH severity)

**Proposed Configuration**:
- Severity: MEDIUM
- Auto-case: No

**Business Rationale**:
- Family accumulator rollup logic may have legitimate variations
- Not always indicative of data corruption
- May be due to different accumulation rules by plan
- Requires investigation but not urgent intervention

**Investigation Required**:
- Verify if family accumulation follows expected mathematical rules
- Check for plan-specific accumulation variations
- Assess if mismatch is within acceptable tolerance

## Implementation Guidelines

### Anomaly Creation Logic
1. **Detection**: Identify condition exists
2. **Severity Assessment**: Apply business rules based on anomaly type
3. **Deduplication**: Check for existing issues with same entity_key
4. **Issue Creation**: Create issue with appropriate severity
5. **Case Creation**: Auto-create support case only for CRITICAL and selected HIGH severity issues

### Entity Key Format
`{member_id}|{plan_id}|{benefit_year}|{subtype}`

This ensures:
- One issue per anomaly type per member/plan/year
- Clear identification of affected entities
- Efficient deduplication

### Monitoring and Review
- **Dashboard Visibility**: All issues should be visible in operational dashboards
- **Trend Analysis**: Track anomaly rates over time
- **False Positive Review**: Regularly assess if anomaly rules generate appropriate signal quality
- **Policy Updates**: Re-evaluate severity levels based on operational experience

## Migration Notes

### Changes from Current Implementation
1. **NEGATIVE_ACCUMULATOR**: HIGH → MEDIUM (reduces support case creation)
2. **FAMILY_ROLLUP_MISMATCH**: HIGH → MEDIUM (reduces urgency)

### Backward Compatibility
- Existing issues maintain their original severity
- Only new anomaly detections use updated severity levels
- Support case creation logic respects updated rules

### Testing Requirements
- Verify negative accumulator scenarios create MEDIUM severity issues
- Confirm CRITICAL anomalies still trigger immediate response
- Validate deduplication continues working
- Test dashboard displays updated severity levels correctly

## Success Metrics

### Signal Quality
- **False Positive Rate**: < 20% of anomalies should be dismissed as non-issues
- **Time to Resolution**: Issues resolved within appropriate SLA by severity
- **Operator Satisfaction**: Anomaly alerts provide actionable insights

### Operational Impact
- **Support Case Quality**: Cases created are genuinely requiring intervention
- **Dashboard Utility**: Dashboard provides trustworthy operational visibility
- **System Trust**: Operations team confidence in anomaly detection accuracy