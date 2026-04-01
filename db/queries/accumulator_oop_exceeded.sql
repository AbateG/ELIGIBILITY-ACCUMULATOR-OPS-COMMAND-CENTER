-- Query 1: Member's Current OOP Accumulator vs Plan Limit
SELECT s.member_id, s.accumulator_type, s.total_amount AS current_oop,
       p.oop_max, p.plan_name,
       (s.total_amount - p.oop_max) AS overage,
       ROUND((s.total_amount * 100.0 / p.oop_max), 1) AS pct_of_max
FROM accumulator_snapshots s
JOIN eligibility_records e ON s.member_id = e.member_id AND e.status = 'active'
JOIN benefit_plans p ON e.plan_id = p.plan_id
WHERE s.member_id = '<MEMBER_ID>'
  AND s.accumulator_type = 'oop'
ORDER BY s.snapshot_date DESC
LIMIT 1;

-- Query 2: Full Transaction History for This Member's OOP
SELECT t.transaction_id, t.member_id, t.accumulator_type,
       t.transaction_amount, t.transaction_date,
       t.claim_record_id, t.created_at,
       c.date_of_service, c.billed_amount, c.allowed_amount,
       c.member_liability
FROM accumulator_transactions t
LEFT JOIN claim_records c ON t.claim_record_id = c.claim_record_id
WHERE t.member_id = '<MEMBER_ID>'
  AND t.accumulator_type = 'oop'
ORDER BY t.transaction_date, t.created_at;

-- Query 3: Running Total to Identify the Breach Point
SELECT t.transaction_id, t.transaction_date, t.transaction_amount,
       t.claim_record_id,
       SUM(t.transaction_amount) OVER (ORDER BY t.transaction_date, t.created_at)
         AS running_total,
       p.oop_max
FROM accumulator_transactions t
JOIN eligibility_records e ON t.member_id = e.member_id AND e.status = 'active'
JOIN benefit_plans p ON e.plan_id = p.plan_id
WHERE t.member_id = '<MEMBER_ID>'
  AND t.accumulator_type = 'oop'
ORDER BY t.transaction_date, t.created_at;