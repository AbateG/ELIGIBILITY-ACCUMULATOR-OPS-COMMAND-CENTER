-- ═══════════════════════════════════════════════════════════════
-- SEED DATA — Matches the unified schema.sql
-- ═══════════════════════════════════════════════════════════════

-- ── CLIENTS ──
INSERT OR IGNORE INTO clients (client_id, client_code, client_name, active_flag, created_at) VALUES
(1, 'CASCADE', 'Cascade Health Alliance',   1, '2025-01-01T00:00:00'),
(2, 'SUMMIT',  'Summit Benefits Group',     1, '2025-01-01T00:00:00'),
(3, 'PRAIRIE', 'Prairie State Health Plan', 1, '2025-01-01T00:00:00'),
(4, 'COASTAL', 'Coastal Care Partners',     1, '2025-01-01T00:00:00');

-- ── VENDORS ──
INSERT OR IGNORE INTO vendors (vendor_id, vendor_code, vendor_name, vendor_type, active_flag, created_at) VALUES
(1, 'MEDIPROC',  'MediProcess Solutions',     'TPA',            1, '2025-01-01T00:00:00'),
(2, 'PHARMBR',   'PharmaBridge Inc',          'PBM',            1, '2025-01-01T00:00:00'),
(3, 'CLEARPATH', 'ClearPath Data Services',   'CLEARINGHOUSE',  1, '2025-01-01T00:00:00'),
(4, 'NATCLAIMS', 'National Claims Network',   'CLAIMS',         1, '2025-01-01T00:00:00');

-- ── CLIENT-VENDOR RELATIONSHIPS ──
INSERT OR IGNORE INTO client_vendor_relationships (relationship_id, client_id, vendor_id, relationship_type, status, created_at) VALUES
(1, 1, 1, 'TPA_SERVICES',       'ACTIVE', '2025-01-01T00:00:00'),
(2, 1, 4, 'CLAIMS_PROCESSING',  'ACTIVE', '2025-01-01T00:00:00'),
(3, 2, 3, 'CLEARINGHOUSE',      'ACTIVE', '2025-01-01T00:00:00'),
(4, 2, 2, 'PBM_SERVICES',       'ACTIVE', '2025-01-01T00:00:00'),
(5, 1, 2, 'PBM_SERVICES',       'ACTIVE', '2025-01-01T00:00:00');

-- ── VENDOR CONTACTS ──
INSERT OR IGNORE INTO vendor_contacts (vendor_id, contact_name, contact_email, contact_phone, contact_type, is_primary, created_at) VALUES
(1, 'Angela Torres',   'a.torres@mediproc.example.com',     '555-0101', 'OPERATIONS', 1, '2025-01-01T00:00:00'),
(1, 'Brian Kozlowski', 'b.kozlowski@mediproc.example.com',  '555-0102', 'TECHNICAL',  0, '2025-01-01T00:00:00'),
(2, 'Carla Nguyen',    'c.nguyen@pharmbr.example.com',      '555-0201', 'OPERATIONS', 1, '2025-01-01T00:00:00'),
(3, 'Derek Simmons',   'd.simmons@clearpath.example.com',   '555-0301', 'OPERATIONS', 1, '2025-01-01T00:00:00'),
(4, 'Elena Marchetti', 'e.marchetti@natclaims.example.com', '555-0401', 'OPERATIONS', 1, '2025-01-01T00:00:00'),
(4, 'Frank Obi',       'f.obi@natclaims.example.com',       '555-0402', 'ESCALATION', 0, '2025-01-01T00:00:00');

-- ── BENEFIT PLANS ──
INSERT OR IGNORE INTO benefit_plans (plan_id, plan_code, plan_name, plan_type, client_id, benefit_year, individual_deductible, family_deductible, individual_oop_max, family_oop_max, coinsurance_rate, family_accumulation_type, active_flag, created_at) VALUES
(1, 'CASCADE-GOLD-PPO',   'Cascade Gold PPO',    'PPO', 1, 2025, 1500.00, 3000.00,  6000.00, 12000.00, 0.20, 'EMBEDDED',  1, '2025-01-01T00:00:00'),
(2, 'CASCADE-SILVER-HMO', 'Cascade Silver HMO',  'HMO', 1, 2025, 3000.00, 6000.00,  8500.00, 17000.00, 0.20, 'EMBEDDED',  1, '2025-01-01T00:00:00'),
(3, 'SUMMIT-STD-PPO',     'Summit Standard PPO',  'PPO', 2, 2025, 2000.00, 4000.00,  7500.00, 15000.00, 0.20, 'EMBEDDED',  1, '2025-01-01T00:00:00');

-- ── MEMBERS ──
INSERT OR IGNORE INTO members (member_id, subscriber_id, client_id, first_name, last_name, dob, gender, relationship_code, family_id, created_at) VALUES
('MBR-001', 'SUB-001', 1, 'Robert',   'Johnson',  '1978-03-15', 'M', 'SUB',    'FAM-001', '2025-01-01T00:00:00'),
('MBR-002', 'SUB-001', 1, 'Maria',    'Johnson',  '1980-07-22', 'F', 'SPOUSE', 'FAM-001', '2025-01-01T00:00:00'),
('MBR-003', 'SUB-001', 1, 'Emma',     'Johnson',  '2005-11-08', 'F', 'CHILD',  'FAM-001', '2025-01-01T00:00:00'),
('MBR-004', 'SUB-001', 1, 'Liam',     'Johnson',  '2008-04-30', 'M', 'CHILD',  'FAM-001', '2025-01-01T00:00:00'),
('MBR-005', 'SUB-005', 1, 'David',    'Chen',     '1982-01-10', 'M', 'SUB',    'FAM-002', '2025-01-01T00:00:00'),
('MBR-006', 'SUB-005', 1, 'Sarah',    'Chen',     '1984-09-05', 'F', 'SPOUSE', 'FAM-002', '2025-01-01T00:00:00'),
('MBR-007', 'SUB-005', 1, 'Olivia',   'Chen',     '2012-06-18', 'F', 'CHILD',  'FAM-002', '2025-01-01T00:00:00'),
('MBR-008', 'SUB-008', 2, 'James',    'Williams', '1975-12-01', 'M', 'SUB',    'FAM-003', '2025-01-01T00:00:00'),
('MBR-009', 'SUB-008', 2, 'Patricia', 'Williams', '1977-05-14', 'F', 'SPOUSE', 'FAM-003', '2025-01-01T00:00:00'),
('MBR-010', 'SUB-008', 2, 'Noah',     'Williams', '2010-08-25', 'M', 'CHILD',  'FAM-003', '2025-01-01T00:00:00'),
('MBR-011', 'SUB-011', 1, 'Emily',    'Davis',    '1990-02-28', 'F', 'SUB',    'FAM-004', '2025-01-01T00:00:00'),
('MBR-012', 'SUB-012', 2, 'Michael',  'Brown',    '1988-10-12', 'M', 'SUB',    'FAM-005', '2025-01-01T00:00:00');

-- ── INBOUND FILES ──
INSERT OR IGNORE INTO inbound_files (file_id, file_name, file_type, client_id, vendor_id, expected_date, received_ts, row_count, processing_status, error_count, created_at) VALUES
(1, 'CASCADE_ELIG_20250610.csv',  'ELIGIBILITY', 1, 1, '2025-06-10', '2025-06-10T05:58:22', 1200, 'PROCESSED', 2,  '2025-06-10T05:58:22'),
(2, 'CASCADE_CLAIMS_20250610.dat','CLAIMS',       1, 4, '2025-06-10', '2025-06-10T07:55:10', 3400, 'PROCESSED', 0,  '2025-06-10T07:55:10'),
(3, 'CASCADE_ELIG_20250611.csv',  'ELIGIBILITY', 1, 1, '2025-06-11', '2025-06-11T06:02:15', 1205, 'PROCESSED', 0,  '2025-06-11T06:02:15'),
(4, 'CASCADE_CLAIMS_20250611.dat','CLAIMS',       1, 4, '2025-06-11', '2025-06-11T08:01:44', 3250, 'PROCESSED', 2,  '2025-06-11T08:01:44'),
(5, 'SUMMIT_ELIG_20250611.csv',   'ELIGIBILITY', 2, 3, '2025-06-11', '2025-06-11T06:55:30',  900, 'PROCESSED', 0,  '2025-06-11T06:55:30'),
(6, 'CASCADE_ACCUM_20250609.csv', 'CLAIMS',       1, 2, '2025-06-09', '2025-06-09T07:05:00',  800, 'PROCESSED', 0,  '2025-06-09T07:05:00');

-- ── PROCESSING RUNS ──
INSERT OR IGNORE INTO processing_runs (run_id, run_type, file_id, started_at, completed_at, run_status, rows_read, rows_passed, rows_failed, issue_count) VALUES
(1, 'ELIGIBILITY_LOAD',    1, '2025-06-10T06:00:00', '2025-06-10T06:12:45', 'SUCCESS', 1200, 1198, 2, 2),
(2, 'CLAIMS_LOAD',         2, '2025-06-10T08:00:00', '2025-06-10T08:30:00', 'SUCCESS', 3400, 3400, 0, 0),
(3, 'ELIGIBILITY_LOAD',    3, '2025-06-11T06:05:00', '2025-06-11T06:15:30', 'SUCCESS', 1205, 1205, 0, 0),
(4, 'CLAIMS_LOAD',         4, '2025-06-11T08:05:00', '2025-06-11T08:28:00', 'SUCCESS', 3250, 3248, 2, 2),
(5, 'ELIGIBILITY_LOAD',    5, '2025-06-11T07:00:00', '2025-06-11T07:10:00', 'SUCCESS',  900,  900, 0, 0),
(6, 'CLAIMS_LOAD',         6, '2025-06-09T07:08:00', '2025-06-09T07:20:00', 'SUCCESS',  800,  800, 0, 0);

-- ── ELIGIBILITY PERIODS ──
INSERT OR IGNORE INTO eligibility_periods (eligibility_id, member_id, subscriber_id, client_id, plan_id, coverage_start, coverage_end, status, source_file_id, created_at) VALUES
( 1, 'MBR-001', 'SUB-001', 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
( 2, 'MBR-002', 'SUB-001', 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
( 3, 'MBR-003', 'SUB-001', 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
( 4, 'MBR-004', 'SUB-001', 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
( 5, 'MBR-005', 'SUB-005', 1, 2, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
( 6, 'MBR-006', 'SUB-005', 1, 2, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
( 7, 'MBR-007', 'SUB-005', 1, 2, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
( 8, 'MBR-008', 'SUB-008', 2, 3, '2025-01-01', '2025-12-31', 'ACTIVE', 5, '2025-01-01T00:00:00'),
( 9, 'MBR-009', 'SUB-008', 2, 3, '2025-01-01', '2025-12-31', 'ACTIVE', 5, '2025-01-01T00:00:00'),
(10, 'MBR-010', 'SUB-008', 2, 3, '2025-01-01', '2025-12-31', 'ACTIVE', 5, '2025-01-01T00:00:00'),
(11, 'MBR-011', 'SUB-011', 1, 1, '2025-01-01', '2025-12-31', 'ACTIVE', 1, '2025-01-01T00:00:00'),
(12, 'MBR-012', 'SUB-012', 2, 3, '2025-01-01', '2025-12-31', 'ACTIVE', 5, '2025-01-01T00:00:00');

-- ── CLAIMS ──
-- Robert Johnson (MBR-001): total member_resp = 2150.00
INSERT OR IGNORE INTO claims (claim_record_id, claim_id, line_id, member_id, subscriber_id, client_id, plan_id, vendor_id, service_date, paid_date, allowed_amount, paid_amount, member_responsibility, deductible_amount, coinsurance_amount, copay_amount, claim_status, source_file_id, created_at) VALUES
(1,  'CLM-2025-10001', '1', 'MBR-001', 'SUB-001', 1, 1, 4, '2025-01-20', '2025-01-21',  360.00,  210.00,  150.00, 150.00, 0.00, 0.00, 'PAID', 2, '2025-01-21T08:00:00'),
(2,  'CLM-2025-10002', '1', 'MBR-001', 'SUB-001', 1, 1, 4, '2025-02-14', '2025-02-15',  680.00,  180.00,  500.00, 500.00, 0.00, 0.00, 'PAID', 2, '2025-02-15T08:00:00'),
(3,  'CLM-2025-10003', '1', 'MBR-001', 'SUB-001', 1, 1, 4, '2025-03-10', '2025-03-11', 1760.00,  260.00, 1500.00, 850.00, 650.00, 0.00, 'PAID', 2, '2025-03-11T08:00:00'),
-- Maria Johnson (MBR-002): total = 875.00
(4,  'CLM-2025-10004', '1', 'MBR-002', 'SUB-001', 1, 1, 4, '2025-02-05', '2025-02-06',  280.00,  155.00,  125.00, 125.00, 0.00, 0.00, 'PAID', 2, '2025-02-06T08:00:00'),
(5,  'CLM-2025-10005', '1', 'MBR-002', 'SUB-001', 1, 1, 4, '2025-04-18', '2025-04-19',  480.00,  105.00,  375.00, 375.00, 0.00, 0.00, 'PAID', 4, '2025-04-19T08:00:00'),
(6,  'CLM-2025-10006', '1', 'MBR-002', 'SUB-001', 1, 1, 4, '2025-05-22', '2025-05-23',  360.00,  235.00,  375.00, 0.00, 375.00, 0.00, 'PAID', 4, '2025-05-23T08:00:00'),
-- Emma Johnson (MBR-003): total = 320.00
(7,  'CLM-2025-10007', '1', 'MBR-003', 'SUB-001', 1, 1, 4, '2025-03-15', '2025-03-16',  240.00,   80.00,  160.00, 160.00, 0.00, 0.00, 'PAID', 2, '2025-03-16T08:00:00'),
(8,  'CLM-2025-10008', '1', 'MBR-003', 'SUB-001', 1, 1, 4, '2025-05-10', '2025-05-11',  200.00,   40.00,  160.00, 160.00, 0.00, 0.00, 'PAID', 4, '2025-05-11T08:00:00'),
-- Liam Johnson (MBR-004): total = 150.00
(9,  'CLM-2025-10009', '1', 'MBR-004', 'SUB-001', 1, 1, 4, '2025-04-02', '2025-04-03',  200.00,   50.00,  150.00, 150.00, 0.00, 0.00, 'PAID', 4, '2025-04-03T08:00:00'),
-- David Chen (MBR-005): total = 4200.00
(10, 'CLM-2025-10010', '1', 'MBR-005', 'SUB-005', 1, 2, 4, '2025-01-15', '2025-01-16', 6800.00, 3800.00, 3000.00, 3000.00, 0.00, 0.00, 'PAID', 2, '2025-01-16T08:00:00'),
(11, 'CLM-2025-10011', '1', 'MBR-005', 'SUB-005', 1, 2, 4, '2025-02-20', '2025-02-21',  280.00,   80.00,  200.00,   0.00, 200.00, 0.00, 'PAID', 2, '2025-02-21T08:00:00'),
(12, 'CLM-2025-10012', '1', 'MBR-005', 'SUB-005', 1, 2, 4, '2025-03-28', '2025-03-29',  400.00,  100.00,  300.00,   0.00, 300.00, 0.00, 'PAID', 2, '2025-03-29T08:00:00'),
(13, 'CLM-2025-10013', '1', 'MBR-005', 'SUB-005', 1, 2, 4, '2025-05-05', '2025-05-06', 2560.00, 1860.00,  700.00,   0.00, 700.00, 0.00, 'PAID', 4, '2025-05-06T08:00:00'),
-- Sarah Chen (MBR-006): total = 1100.00
(14, 'CLM-2025-10014', '1', 'MBR-006', 'SUB-005', 1, 2, 4, '2025-02-10', '2025-02-11',  480.00,  130.00,  350.00, 350.00, 0.00, 0.00, 'PAID', 2, '2025-02-11T08:00:00'),
(15, 'CLM-2025-10015', '1', 'MBR-006', 'SUB-005', 1, 2, 4, '2025-04-08', '2025-04-09',  360.00,  110.00,  250.00, 250.00, 0.00, 0.00, 'PAID', 4, '2025-04-09T08:00:00'),
(16, 'CLM-2025-10016', '1', 'MBR-006', 'SUB-005', 1, 2, 4, '2025-05-30', '2025-05-31', 1440.00,  940.00,  500.00,   0.00, 500.00, 0.00, 'PAID', 4, '2025-05-31T08:00:00'),
-- Olivia Chen (MBR-007): total = 450.00
(17, 'CLM-2025-10017', '1', 'MBR-007', 'SUB-005', 1, 2, 4, '2025-03-22', '2025-03-23',  240.00,   40.00,  200.00, 200.00, 0.00, 0.00, 'PAID', 2, '2025-03-23T08:00:00'),
(18, 'CLM-2025-10018', '1', 'MBR-007', 'SUB-005', 1, 2, 4, '2025-05-15', '2025-05-16',  200.00,  -50.00,  250.00, 250.00, 0.00, 0.00, 'PAID', 4, '2025-05-16T08:00:00'),
-- James Williams (MBR-008): total = 1800.00
(19, 'CLM-2025-10019', '1', 'MBR-008', 'SUB-008', 2, 3, 4, '2025-01-28', '2025-01-29',  360.00,  110.00,  250.00, 250.00, 0.00, 0.00, 'PAID', 2, '2025-01-29T08:00:00'),
(20, 'CLM-2025-10020', '1', 'MBR-008', 'SUB-008', 2, 3, 4, '2025-02-25', '2025-02-26', 9600.00, 7600.00, 1200.00, 1200.00, 0.00, 0.00, 'PAID', 2, '2025-02-26T08:00:00'),
(21, 'CLM-2025-10021', '1', 'MBR-008', 'SUB-008', 2, 3, 4, '2025-04-15', '2025-04-16',  360.00,   10.00,  350.00,   0.00, 350.00, 0.00, 'PAID', 4, '2025-04-16T08:00:00'),
-- Patricia Williams (MBR-009): total = 600.00
(22, 'CLM-2025-10022', '1', 'MBR-009', 'SUB-008', 2, 3, 4, '2025-03-05', '2025-03-06',  400.00,  100.00,  300.00, 300.00, 0.00, 0.00, 'PAID', 2, '2025-03-06T08:00:00'),
(23, 'CLM-2025-10023', '1', 'MBR-009', 'SUB-008', 2, 3, 4, '2025-05-18', '2025-05-19',  440.00,  140.00,  300.00, 300.00, 0.00, 0.00, 'PAID', 4, '2025-05-19T08:00:00'),
-- Noah Williams (MBR-010): total = 200.00
(24, 'CLM-2025-10024', '1', 'MBR-010', 'SUB-008', 2, 3, 4, '2025-04-28', '2025-04-29',  200.00,    0.00,  200.00, 200.00, 0.00, 0.00, 'PAID', 4, '2025-04-29T08:00:00'),
-- Emily Davis (MBR-011): total = 3400.00
(25, 'CLM-2025-10025', '1', 'MBR-011', 'SUB-011', 1, 1, 4, '2025-01-10', '2025-01-11',  280.00,   80.00,  200.00, 200.00, 0.00, 0.00, 'PAID', 2, '2025-01-11T08:00:00'),
(26, 'CLM-2025-10026', '1', 'MBR-011', 'SUB-011', 1, 1, 4, '2025-02-08', '2025-02-09', 6000.00, 4700.00, 1300.00, 1300.00, 0.00, 0.00, 'PAID', 2, '2025-02-09T08:00:00'),
(27, 'CLM-2025-10027', '1', 'MBR-011', 'SUB-011', 1, 1, 4, '2025-03-20', '2025-03-21',  280.00,   80.00,  200.00,   0.00, 200.00, 0.00, 'PAID', 2, '2025-03-21T08:00:00'),
(28, 'CLM-2025-10028', '1', 'MBR-011', 'SUB-011', 1, 1, 4, '2025-04-22', '2025-04-23',  160.00,   10.00,  150.00,   0.00, 150.00, 0.00, 'PAID', 4, '2025-04-23T08:00:00'),
(29, 'CLM-2025-10029', '1', 'MBR-011', 'SUB-011', 1, 1, 4, '2025-05-01', '2025-05-02', 1760.00,  210.00, 1550.00,   0.00, 1550.00, 0.00, 'PAID', 4, '2025-05-02T08:00:00'),
-- Michael Brown (MBR-012): total = 950.00
(30, 'CLM-2025-10030', '1', 'MBR-012', 'SUB-012', 2, 3, 4, '2025-02-18', '2025-02-19',  400.00,   50.00,  350.00, 350.00, 0.00, 0.00, 'PAID', 2, '2025-02-19T08:00:00'),
(31, 'CLM-2025-10031', '1', 'MBR-012', 'SUB-012', 2, 3, 4, '2025-04-10', '2025-04-11',  640.00,   40.00,  600.00, 600.00, 0.00, 0.00, 'PAID', 4, '2025-04-11T08:00:00');

-- ── ACCUMULATOR SNAPSHOTS ──
INSERT OR IGNORE INTO accumulator_snapshots (snapshot_id, member_id, family_id, client_id, plan_id, benefit_year, individual_deductible_accum, family_deductible_accum, individual_oop_accum, family_oop_accum, individual_deductible_met_flag, family_deductible_met_flag, individual_oop_met_flag, family_oop_met_flag, snapshot_ts) VALUES
-- Johnson family (plan 1, FAM-001)
(1,  'MBR-001', 'FAM-001', 1, 1, 2025, 1500.00, 2845.00, 2150.00, 3495.00, 1, 0, 0, 0, '2025-06-15T08:00:00'),
(2,  'MBR-002', 'FAM-001', 1, 1, 2025,  500.00, 2845.00,  875.00, 3495.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
(3,  'MBR-003', 'FAM-001', 1, 1, 2025,  320.00, 2845.00,  320.00, 3495.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
(4,  'MBR-004', 'FAM-001', 1, 1, 2025,  150.00, 2845.00,  150.00, 3495.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
-- Chen family (plan 2, FAM-002)
(5,  'MBR-005', 'FAM-002', 1, 2, 2025, 3000.00, 3600.00, 4200.00, 5750.00, 1, 0, 0, 0, '2025-06-15T08:00:00'),
(6,  'MBR-006', 'FAM-002', 1, 2, 2025,  600.00, 3600.00, 1100.00, 5750.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
(7,  'MBR-007', 'FAM-002', 1, 2, 2025,  450.00, 3600.00,  450.00, 5750.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
-- Williams family (plan 3, FAM-003)
(8,  'MBR-008', 'FAM-003', 2, 3, 2025, 1450.00, 2050.00, 1800.00, 2600.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
(9,  'MBR-009', 'FAM-003', 2, 3, 2025,  600.00, 2050.00,  600.00, 2600.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
(10, 'MBR-010', 'FAM-003', 2, 3, 2025,  200.00, 2050.00,  200.00, 2600.00, 0, 0, 0, 0, '2025-06-15T08:00:00'),
-- Individual members
(11, 'MBR-011', 'FAM-004', 1, 1, 2025, 1500.00, 1500.00, 3400.00, 3400.00, 1, 1, 0, 0, '2025-06-15T08:00:00'),
(12, 'MBR-012', 'FAM-005', 2, 3, 2025,  950.00,  950.00,  950.00,  950.00, 0, 0, 0, 0, '2025-06-15T08:00:00');

-- ── FILE SCHEDULES ──
INSERT OR IGNORE INTO file_schedules (schedule_id, client_id, vendor_id, file_type, file_direction, frequency, expected_time, day_of_week, is_active, created_at) VALUES
(1, 1, 1, 'ELIGIBILITY', 'INBOUND',  'DAILY',  '06:00:00', NULL,  1, '2025-01-01T00:00:00'),
(2, 1, 4, 'CLAIMS',      'INBOUND',  'DAILY',  '08:00:00', NULL,  1, '2025-01-01T00:00:00'),
(3, 1, 2, 'CLAIMS',      'INBOUND',  'WEEKLY', '07:00:00', 'MON', 1, '2025-01-01T00:00:00'),
(4, 1, 1, 'ELIGIBILITY', 'OUTBOUND', 'DAILY',  '18:00:00', NULL,  1, '2025-01-01T00:00:00'),
(5, 2, 3, 'ELIGIBILITY', 'INBOUND',  'DAILY',  '07:00:00', NULL,  1, '2025-01-01T00:00:00'),
(6, 2, 3, 'CLAIMS',      'INBOUND',  'DAILY',   '09:00:00', NULL,  1, '2025-01-01T00:00:00'),
(7, 2, 2, 'CLAIMS',      'INBOUND',  'WEEKLY', '07:00:00', 'TUE', 1, '2025-01-01T00:00:00');

-- ── BASELINE DATA QUALITY ISSUES ──
INSERT OR IGNORE INTO data_quality_issues (issue_id, issue_code, issue_type, issue_subtype, severity, status, client_id, vendor_id, file_id, run_id, member_id, issue_message, issue_description, detected_at, created_at) VALUES
(1, 'ELIG_MISSING_FIELD',   'ELIGIBILITY', 'MISSING_DATA',    'MEDIUM', 'OPEN', 1, 1, 1, 1, 'MBR-001', 'Missing group_id for member MBR-001',            'Required field group_id is null or empty',                  '2025-06-10T06:05:00', '2025-06-10T06:05:00'),
(2, 'ELIG_INVALID_DATE',    'ELIGIBILITY', 'INVALID_FORMAT',  'HIGH',   'OPEN', 1, 1, 1, 1, 'MBR-003', 'Invalid coverage_end date format for MBR-003',    'Date format does not match expected YYYY-MM-DD pattern',    '2025-06-10T06:06:00', '2025-06-10T06:06:00'),
(3, 'CLAIM_DUP_LINE',       'CLAIMS',      'DUPLICATE',       'LOW',    'RESOLVED', 1, 4, 4, 4, NULL,   'Duplicate claim line detected CLM-2025-10020-1',  'Exact duplicate claim line submitted in same file',         '2025-06-11T08:10:00', '2025-06-11T08:10:00'),
(4, 'CLAIM_AMOUNT_MISMATCH','CLAIMS',      'BUSINESS_RULE',   'HIGH',   'OPEN', 2, 4, 4, 4, 'MBR-008', 'Claim amount mismatch for James Williams',   'Paid + member_responsibility != allowed_amount',     '2025-06-11T08:12:00', '2025-06-11T08:12:00');

-- ── BASELINE SUPPORT CASES ──
INSERT OR IGNORE INTO support_cases (case_id, case_number, issue_id, client_id, vendor_id, file_id, run_id, case_type, priority, severity, status, assigned_team, assigned_to, short_description, description, opened_at, created_at, updated_at) VALUES
(1, 'CASE-2025-0001', 2, 1, 1, 1, 1, 'DATA_QUALITY', 'P2', 'HIGH',   'OPEN',         'Eligibility Ops', 'Jane Smith', 'Invalid date format in eligibility file',    'coverage_end has non-standard date format for MBR-003 in CASCADE_ELIG_20250610.csv', '2025-06-10T06:10:00', '2025-06-10T06:10:00', '2025-06-10T06:10:00'),
(2, 'CASE-2025-0002', 4, 2, 4, 4, 4, 'DATA_QUALITY', 'P1', 'HIGH',   'ACKNOWLEDGED', 'Claims Ops',      'Bob Lee',    'Claim amount mismatch for James Williams',   'Paid + member_responsibility does not balance to allowed_amount for CLM-2025-10020', '2025-06-11T08:15:00', '2025-06-11T08:15:00', '2025-06-11T09:00:00');

-- ── BASELINE SLA TRACKING ──
INSERT OR IGNORE INTO sla_tracking (sla_id, case_id, sla_type, target_hours, target_due_at, status, is_at_risk, is_breached, last_evaluated_at, created_at, updated_at) VALUES
(1, 1, 'RESPONSE',   4,  '2025-06-10T10:10:00', 'OPEN', 0, 0, '2025-06-10T06:10:00', '2025-06-10T06:10:00', '2025-06-10T06:10:00'),
(2, 1, 'RESOLUTION', 24, '2025-06-11T06:10:00', 'OPEN', 1, 0, '2025-06-10T06:10:00', '2025-06-10T06:10:00', '2025-06-10T06:10:00'),
(3, 2, 'RESPONSE',   4,  '2025-06-11T12:15:00', 'MET',  0, 0, '2025-06-11T09:00:00', '2025-06-11T08:15:00', '2025-06-11T09:00:00'),
(4, 2, 'RESOLUTION', 24, '2025-06-12T08:15:00', 'OPEN', 0, 0, '2025-06-11T09:00:00', '2025-06-11T08:15:00', '2025-06-11T08:15:00');

-- ── AUDIT LOG ──
INSERT OR IGNORE INTO audit_log (audit_id, event_ts, event_type, entity_name, entity_key, run_id, file_id, actor, event_details) VALUES
(1, '2025-06-10T05:58:22', 'FILE_REGISTERED',        'inbound_files', '1', NULL, 1, 'system', 'Registered CASCADE_ELIG_20250610.csv'),
(2, '2025-06-10T06:12:45', 'VALIDATION_COMPLETED',   'processing_runs', '1', 1, 1, 'system', 'Eligibility load completed with 2 issues'),
(3, '2025-06-10T07:55:10', 'FILE_REGISTERED',        'inbound_files', '2', NULL, 2, 'system', 'Registered CASCADE_CLAIMS_20250610.dat'),
(4, '2025-06-10T08:30:00', 'VALIDATION_COMPLETED',   'processing_runs', '2', 2, 2, 'system', 'Claims load completed successfully'),
(5, '2025-06-10T06:10:00', 'CASE_CREATED',           'support_cases', '1', 1, 1, 'system', 'Auto-generated case for invalid date format issue');