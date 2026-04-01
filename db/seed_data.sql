-- ═══════════════════════════════════════════════════════════════
-- SEED DATA — Eligibility & Accumulator Operations Command Center
-- ═══════════════════════════════════════════════════════════════
--
-- Purpose: Populate reference/dimensional tables so the Streamlit
--          dashboards render meaningful baseline data BEFORE any
--          scenarios are run.
--
-- Load order matters — foreign key dependencies flow top-down:
--   clients → vendors → client_vendor_relationships → vendor_contacts
--   → benefit_plans → members → eligibility_periods → file_schedules
--
-- All data is synthetic. No real PHI or entity names.
-- Dates use 2025 benefit year. Adjust if your context differs.
--
-- Usage:
--   sqlite3 data/operations.db < db/seed_data.sql
--   — or —
--   Called automatically by src/db/init_db.py after schema.sql
-- ═══════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────
-- CLIENTS (Health Plans)
-- ───────────────────────────────────────────────────────────────

INSERT OR IGNORE INTO clients (client_id, client_code, client_name, active_flag, created_at) VALUES
(1, 'CASCADE', 'Cascade Health Alliance', 1, '2025-01-01T00:00:00'),
(2, 'SUMMIT', 'Summit Benefits Group', 1, '2025-01-01T00:00:00'),
(3, 'PRAIRIE', 'Prairie State Health Plan', 1, '2025-01-01T00:00:00'),
(4, 'COASTAL', 'Coastal Care Partners', 1, '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- VENDORS (TPAs, PBMs, Clearinghouses, Claims Processors)
-- ───────────────────────────────────────────────────────────────

INSERT OR IGNORE INTO vendors (vendor_id, vendor_code, vendor_name, vendor_type, active_flag, created_at) VALUES
(1, 'MEDIPROC', 'MediProcess Solutions', 'TPA', 1, '2025-01-01T00:00:00'),
(2, 'PHARMBR', 'PharmaBridge Inc', 'PBM', 1, '2025-01-01T00:00:00'),
(3, 'CLEARPATH', 'ClearPath Data Services', 'CLEARINGHOUSE', 1, '2025-01-01T00:00:00'),
(4, 'NATCLAIMS', 'National Claims Network', 'CLAIMS', 1, '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- CLIENT–VENDOR RELATIONSHIPS
-- ───────────────────────────────────────────────────────────────
-- Cascade works with MediProcess (TPA) and National Claims
-- Summit  works with ClearPath and PharmaBridge

INSERT OR IGNORE INTO client_vendor_relationships (relationship_id, client_id, vendor_id, relationship_type, status, created_at) VALUES
(1, 1, 1, 'TPA_SERVICES', 'ACTIVE', '2025-01-01T00:00:00'),
(2, 1, 4, 'CLAIMS_PROCESSING', 'ACTIVE', '2025-01-01T00:00:00'),
(3, 2, 3, 'CLEARINGHOUSE', 'ACTIVE', '2025-01-01T00:00:00'),
(4, 2, 2, 'PBM_SERVICES', 'ACTIVE', '2025-01-01T00:00:00'),
(5, 1, 2, 'PBM_SERVICES', 'ACTIVE', '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- VENDOR CONTACTS
-- ───────────────────────────────────────────────────────────────

INSERT OR IGNORE INTO vendor_contacts (contact_id, vendor_id, contact_name, contact_email, contact_phone, contact_type, is_primary, created_at) VALUES
('CON-001', 'VND-001', 'Angela Torres',   'a.torres@mediproc.example.com',    '555-0101', 'OPERATIONS',  1, '2025-01-01T00:00:00'),
('CON-002', 'VND-001', 'Brian Kozlowski', 'b.kozlowski@mediproc.example.com', '555-0102', 'TECHNICAL',   0, '2025-01-01T00:00:00'),
('CON-003', 'VND-002', 'Carla Nguyen',    'c.nguyen@pharmbr.example.com',     '555-0201', 'OPERATIONS',  1, '2025-01-01T00:00:00'),
('CON-004', 'VND-003', 'Derek Simmons',   'd.simmons@clearpath.example.com',  '555-0301', 'OPERATIONS',  1, '2025-01-01T00:00:00'),
('CON-005', 'VND-004', 'Elena Marchetti', 'e.marchetti@natclaims.example.com','555-0401', 'OPERATIONS',  1, '2025-01-01T00:00:00'),
('CON-006', 'VND-004', 'Frank Obi',       'f.obi@natclaims.example.com',      '555-0402', 'ESCALATION',  0, '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- BENEFIT PLANS
-- ───────────────────────────────────────────────────────────────
-- Realistic OOP max and deductible amounts for different plan tiers

INSERT OR IGNORE INTO benefit_plans (plan_id, plan_name, plan_type, client_id, oop_max_individual, oop_max_family, deductible_individual, deductible_family, benefit_year_start, benefit_year_end, status, created_at, updated_at) VALUES
('PLN-001', 'Cascade Gold PPO',     'PPO', 'CLT-001', 6000.00, 12000.00, 1500.00, 3000.00, '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('PLN-002', 'Cascade Silver HMO',   'HMO', 'CLT-001', 8500.00, 17000.00, 3000.00, 6000.00, '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('PLN-003', 'Summit Standard PPO',  'PPO', 'CLT-002', 7500.00, 15000.00, 2000.00, 4000.00, '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00', '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- MEMBERS
-- ───────────────────────────────────────────────────────────────
-- 3 families + 2 individual subscribers = 12 members
--
-- Family FAM-001: Johnson (4 members, Cascade Gold PPO)
-- Family FAM-002: Chen    (3 members, Cascade Silver HMO)
-- Family FAM-003: Williams(3 members, Summit Standard PPO)
-- Individual:     Davis   (Cascade Gold PPO)
-- Individual:     Brown   (Summit Standard PPO)

INSERT OR IGNORE INTO members (member_id, external_member_id, first_name, last_name, date_of_birth, gender, family_id, relationship_code, client_id, plan_id, eligibility_status, created_at, updated_at) VALUES
-- Johnson Family
('MBR-001', 'EXT-90001', 'Robert',   'Johnson',  '1978-03-15', 'M', 'FAM-001', 'SUBSCRIBER', 'CLT-001', 'PLN-001', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-002', 'EXT-90002', 'Maria',    'Johnson',  '1980-07-22', 'F', 'FAM-001', 'SPOUSE',     'CLT-001', 'PLN-001', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-003', 'EXT-90003', 'Emma',     'Johnson',  '2005-11-08', 'F', 'FAM-001', 'DEPENDENT',  'CLT-001', 'PLN-001', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-004', 'EXT-90004', 'Liam',     'Johnson',  '2008-04-30', 'M', 'FAM-001', 'DEPENDENT',  'CLT-001', 'PLN-001', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),

-- Chen Family
('MBR-005', 'EXT-90005', 'David',    'Chen',     '1982-01-10', 'M', 'FAM-002', 'SUBSCRIBER', 'CLT-001', 'PLN-002', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-006', 'EXT-90006', 'Sarah',    'Chen',     '1984-09-05', 'F', 'FAM-002', 'SPOUSE',     'CLT-001', 'PLN-002', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-007', 'EXT-90007', 'Olivia',   'Chen',     '2012-06-18', 'F', 'FAM-002', 'DEPENDENT',  'CLT-001', 'PLN-002', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),

-- Williams Family
('MBR-008', 'EXT-90008', 'James',    'Williams', '1975-12-01', 'M', 'FAM-003', 'SUBSCRIBER', 'CLT-002', 'PLN-003', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-009', 'EXT-90009', 'Patricia', 'Williams', '1977-05-14', 'F', 'FAM-003', 'SPOUSE',     'CLT-002', 'PLN-003', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-010', 'EXT-90010', 'Noah',     'Williams', '2010-08-25', 'M', 'FAM-003', 'DEPENDENT',  'CLT-002', 'PLN-003', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),

-- Individual Members (no family dependents)
('MBR-011', 'EXT-90011', 'Emily',    'Davis',    '1990-02-28', 'F', 'FAM-004', 'SUBSCRIBER', 'CLT-001', 'PLN-001', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00'),
('MBR-012', 'EXT-90012', 'Michael',  'Brown',    '1988-10-12', 'M', 'FAM-005', 'SUBSCRIBER', 'CLT-002', 'PLN-003', 'ACTIVE',     '2025-01-01T00:00:00', '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- ELIGIBILITY PERIODS
-- ───────────────────────────────────────────────────────────────
-- Every member has an active eligibility period for the 2025 benefit year

INSERT OR IGNORE INTO eligibility_periods (eligibility_period_id, member_id, plan_id, start_date, end_date, status, created_at) VALUES
('ELG-001', 'MBR-001', 'PLN-001', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-002', 'MBR-002', 'PLN-001', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-003', 'MBR-003', 'PLN-001', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-004', 'MBR-004', 'PLN-001', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-005', 'MBR-005', 'PLN-002', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-006', 'MBR-006', 'PLN-002', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-007', 'MBR-007', 'PLN-002', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-008', 'MBR-008', 'PLN-003', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-009', 'MBR-009', 'PLN-003', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-010', 'MBR-010', 'PLN-003', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-011', 'MBR-011', 'PLN-001', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00'),
('ELG-012', 'MBR-012', 'PLN-003', '2025-01-01', '2025-12-31', 'ACTIVE', '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- ACCUMULATOR SNAPSHOTS (Baseline — mid-year realistic amounts)
-- ───────────────────────────────────────────────────────────────
-- Each member starts with some YTD accumulation so dashboards
-- show progress bars and charts on first load.
-- Family-level accumulators for FAM-001 and FAM-002 are also seeded.

INSERT OR IGNORE INTO accumulator_snapshots (accumulator_id, member_id, plan_id, accumulator_type, period_start, period_end, current_amount, limit_amount, last_updated_at, created_at) VALUES
-- Johnson Family: Individual OOP
('ACC-001', 'MBR-001', 'PLN-001', 'oop_individual', '2025-01-01', '2025-12-31', 2150.00, 6000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-002', 'MBR-002', 'PLN-001', 'oop_individual', '2025-01-01', '2025-12-31',  875.00, 6000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-003', 'MBR-003', 'PLN-001', 'oop_individual', '2025-01-01', '2025-12-31',  320.00, 6000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-004', 'MBR-004', 'PLN-001', 'oop_individual', '2025-01-01', '2025-12-31',  150.00, 6000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
-- Johnson Family: Family OOP (sum of individuals = 3495.00)
('ACC-005', 'MBR-001', 'PLN-001', 'oop_family',     '2025-01-01', '2025-12-31', 3495.00, 12000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),

-- Johnson Family: Individual Deductible
('ACC-006', 'MBR-001', 'PLN-001', 'deductible_individual', '2025-01-01', '2025-12-31', 1500.00, 1500.00, '2025-03-10T08:00:00', '2025-01-15T00:00:00'),
('ACC-007', 'MBR-002', 'PLN-001', 'deductible_individual', '2025-01-01', '2025-12-31',  875.00, 1500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-008', 'MBR-003', 'PLN-001', 'deductible_individual', '2025-01-01', '2025-12-31',  320.00, 1500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-009', 'MBR-004', 'PLN-001', 'deductible_individual', '2025-01-01', '2025-12-31',  150.00, 1500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
-- Johnson Family: Family Deductible
('ACC-010', 'MBR-001', 'PLN-001', 'deductible_family',     '2025-01-01', '2025-12-31', 2845.00, 3000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),

-- Chen Family: Individual OOP
('ACC-011', 'MBR-005', 'PLN-002', 'oop_individual', '2025-01-01', '2025-12-31', 4200.00, 8500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-012', 'MBR-006', 'PLN-002', 'oop_individual', '2025-01-01', '2025-12-31', 1100.00, 8500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-013', 'MBR-007', 'PLN-002', 'oop_individual', '2025-01-01', '2025-12-31',  450.00, 8500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
-- Chen Family: Family OOP (sum = 5750.00)
('ACC-014', 'MBR-005', 'PLN-002', 'oop_family',     '2025-01-01', '2025-12-31', 5750.00, 17000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),

-- Williams Family: Individual OOP
('ACC-015', 'MBR-008', 'PLN-003', 'oop_individual', '2025-01-01', '2025-12-31', 1800.00, 7500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-016', 'MBR-009', 'PLN-003', 'oop_individual', '2025-01-01', '2025-12-31',  600.00, 7500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-017', 'MBR-010', 'PLN-003', 'oop_individual', '2025-01-01', '2025-12-31',  200.00, 7500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
-- Williams Family: Family OOP (sum = 2600.00)
('ACC-018', 'MBR-008', 'PLN-003', 'oop_family',     '2025-01-01', '2025-12-31', 2600.00, 15000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),

-- Individual Members
('ACC-019', 'MBR-011', 'PLN-001', 'oop_individual', '2025-01-01', '2025-12-31', 3400.00, 6000.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00'),
('ACC-020', 'MBR-012', 'PLN-003', 'oop_individual', '2025-01-01', '2025-12-31',  950.00, 7500.00, '2025-06-15T08:00:00', '2025-01-15T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- FILE SCHEDULES
-- ───────────────────────────────────────────────────────────────
-- Define expected file deliveries so File Monitoring can detect
-- missing files before any scenarios are run.

INSERT OR IGNORE INTO file_schedules (schedule_id, client_id, vendor_id, file_type, file_direction, frequency, expected_time, day_of_week, day_of_month, is_active, created_at) VALUES
-- Cascade Health Alliance feeds
('SCH-001', 'CLT-001', 'VND-001', 'ELIGIBILITY',  'INBOUND',  'DAILY',   '06:00:00', NULL, NULL, 1, '2025-01-01T00:00:00'),
('SCH-002', 'CLT-001', 'VND-004', 'CLAIMS',       'INBOUND',  'DAILY',   '08:00:00', NULL, NULL, 1, '2025-01-01T00:00:00'),
('SCH-003', 'CLT-001', 'VND-002', 'ACCUMULATOR',  'INBOUND',  'WEEKLY',  '07:00:00', 'MON', NULL, 1, '2025-01-01T00:00:00'),
('SCH-004', 'CLT-001', 'VND-001', 'ELIGIBILITY',  'OUTBOUND', 'DAILY',   '18:00:00', NULL, NULL, 1, '2025-01-01T00:00:00'),

-- Summit Benefits Group feeds
('SCH-005', 'CLT-002', 'VND-003', 'ELIGIBILITY',  'INBOUND',  'DAILY',   '07:00:00', NULL, NULL, 1, '2025-01-01T00:00:00'),
('SCH-006', 'CLT-002', 'VND-003', 'CLAIMS',       'INBOUND',  'DAILY',   '09:00:00', NULL, NULL, 1, '2025-01-01T00:00:00'),
('SCH-007', 'CLT-002', 'VND-002', 'ACCUMULATOR',  'INBOUND',  'WEEKLY',  '07:00:00', 'TUE', NULL, 1, '2025-01-01T00:00:00');


-- ───────────────────────────────────────────────────────────────
-- BASELINE FILE INVENTORY
-- ───────────────────────────────────────────────────────────────
-- A few recent successfully-processed files so the Home dashboard
-- shows file status distribution and File Monitoring has history.

INSERT OR IGNORE INTO file_inventory (file_id, file_name, file_type, file_direction, file_status, client_id, vendor_id, schedule_id, expected_date, received_at, file_size, record_count, processing_run_id, created_at, updated_at) VALUES
('FIL-SEED-001', 'CASCADE_ELIG_20250610.csv',  'ELIGIBILITY', 'INBOUND', 'PROCESSED', 'CLT-001', 'VND-001', 'SCH-001', '2025-06-10', '2025-06-10T05:58:22', 245000, 1200, 'RUN-SEED-001', '2025-06-10T05:58:22', '2025-06-10T06:12:45'),
('FIL-SEED-002', 'CASCADE_CLAIMS_20250610.dat', 'CLAIMS',      'INBOUND', 'PROCESSED', 'CLT-001', 'VND-004', 'SCH-002', '2025-06-10', '2025-06-10T07:55:10', 512000, 3400, 'RUN-SEED-002', '2025-06-10T07:55:10', '2025-06-10T08:30:00'),
('FIL-SEED-003', 'CASCADE_ELIG_20250611.csv',  'ELIGIBILITY', 'INBOUND', 'PROCESSED', 'CLT-001', 'VND-001', 'SCH-001', '2025-06-11', '2025-06-11T06:02:15', 246000, 1205, 'RUN-SEED-003', '2025-06-11T06:02:15', '2025-06-11T06:15:30'),
('FIL-SEED-004', 'CASCADE_CLAIMS_20250611.dat', 'CLAIMS',      'INBOUND', 'PROCESSED', 'CLT-001', 'VND-004', 'SCH-002', '2025-06-11', '2025-06-11T08:01:44', 498000, 3250, 'RUN-SEED-004', '2025-06-11T08:01:44', '2025-06-11T08:28:00'),
('FIL-SEED-005', 'SUMMIT_ELIG_20250611.csv',   'ELIGIBILITY', 'INBOUND', 'PROCESSED', 'CLT-002', 'VND-003', 'SCH-005', '2025-06-11', '2025-06-11T06:55:30', 180000,  900, 'RUN-SEED-005', '2025-06-11T06:55:30', '2025-06-11T07:10:00'),
('FIL-SEED-006', 'CASCADE_ACCUM_20250609.csv', 'ACCUMULATOR', 'INBOUND', 'PROCESSED', 'CLT-001', 'VND-002', 'SCH-003', '2025-06-09', '2025-06-09T07:05:00', 125000,  800, 'RUN-SEED-006', '2025-06-09T07:05:00', '2025-06-09T07:20:00');


-- ───────────────────────────────────────────────────────────────
-- BASELINE PROCESSING RUNS
-- ───────────────────────────────────────────────────────────────
-- Corresponding to the baseline files above. All successful.

INSERT OR IGNORE INTO processing_runs (processing_run_id, file_id, run_type, run_status, started_at, completed_at, records_processed, records_succeeded, records_failed, error_message, created_at) VALUES
('RUN-SEED-001', 'FIL-SEED-001', 'ELIGIBILITY_LOAD', 'SUCCESS', '2025-06-10T06:00:00', '2025-06-10T06:12:45', 1200, 1198, 2,  NULL, '2025-06-10T06:00:00'),
('RUN-SEED-002', 'FIL-SEED-002', 'CLAIMS_LOAD',      'SUCCESS', '2025-06-10T08:00:00', '2025-06-10T08:30:00', 3400, 3400, 0,  NULL, '2025-06-10T08:00:00'),
('RUN-SEED-003', 'FIL-SEED-003', 'ELIGIBILITY_LOAD', 'SUCCESS', '2025-06-11T06:05:00', '2025-06-11T06:15:30', 1205, 1205, 0,  NULL, '2025-06-11T06:05:00'),
('RUN-SEED-004', 'FIL-SEED-004', 'CLAIMS_LOAD',      'SUCCESS', '2025-06-11T08:05:00', '2025-06-11T08:28:00', 3250, 3248, 2,  NULL, '2025-06-11T08:05:00'),
('RUN-SEED-005', 'FIL-SEED-005', 'ELIGIBILITY_LOAD', 'SUCCESS', '2025-06-11T07:00:00', '2025-06-11T07:10:00',  900,  900, 0,  NULL, '2025-06-11T07:00:00'),
('RUN-SEED-006', 'FIL-SEED-006', 'ACCUMULATOR_LOAD', 'SUCCESS', '2025-06-09T07:08:00', '2025-06-09T07:20:00',  800,  800, 0,  NULL, '2025-06-09T07:08:00');


-- ───────────────────────────────────────────────────────────────
-- BASELINE CLAIM RECORDS
-- ───────────────────────────────────────────────────────────────
-- Enough claims to justify the accumulator snapshot amounts above
-- and give the Issue Triage and Accumulator pages data to render.
-- Amounts chosen so individual claim member_responsibility sums
-- match the accumulator snapshots.

INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
-- Robert Johnson (MBR-001): total member_resp = 2150.00
('CLM-SEED-001', 'MBR-001', 'CLM-2025-10001', '2025-01-20', 'Cascade Medical Group',     '1234567890', 'M79.3',  '99213', 450.00,  360.00,  210.00,  150.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-01-21T08:00:00', '2025-01-21T08:00:00'),
('CLM-SEED-002', 'MBR-001', 'CLM-2025-10002', '2025-02-14', 'Valley Orthopedics',        '1234567891', 'M54.5',  '99214', 850.00,  680.00,  180.00,  500.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-02-15T08:00:00', '2025-02-15T08:00:00'),
('CLM-SEED-003', 'MBR-001', 'CLM-2025-10003', '2025-03-10', 'Cascade Imaging Center',    '1234567892', 'M54.5',  '72148', 2200.00, 1760.00,  260.00, 1500.00, 'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-03-11T08:00:00', '2025-03-11T08:00:00'),

-- Maria Johnson (MBR-002): total member_resp = 875.00
('CLM-SEED-004', 'MBR-002', 'CLM-2025-10004', '2025-02-05', 'Cascade Medical Group',     '1234567890', 'J06.9',  '99213', 350.00,  280.00,  155.00,  125.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-02-06T08:00:00', '2025-02-06T08:00:00'),
('CLM-SEED-005', 'MBR-002', 'CLM-2025-10005', '2025-04-18', 'Women''s Health Clinic',     '1234567893', 'Z01.419','99395', 600.00,  480.00,  105.00,  375.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-04-19T08:00:00', '2025-04-19T08:00:00'),
('CLM-SEED-006', 'MBR-002', 'CLM-2025-10006', '2025-05-22', 'Cascade Medical Group',     '1234567890', 'R10.9',  '99213', 450.00,  360.00,  235.00,  375.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-05-23T08:00:00', '2025-05-23T08:00:00'),

-- Emma Johnson (MBR-003): total member_resp = 320.00
('CLM-SEED-007', 'MBR-003', 'CLM-2025-10007', '2025-03-15', 'Pediatric Associates',      '1234567894', 'J20.9',  '99213', 300.00,  240.00,   80.00,  160.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-03-16T08:00:00', '2025-03-16T08:00:00'),
('CLM-SEED-008', 'MBR-003', 'CLM-2025-10008', '2025-05-10', 'Pediatric Associates',      '1234567894', 'Z00.129','99394', 250.00,  200.00,   40.00,  160.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-05-11T08:00:00', '2025-05-11T08:00:00'),

-- Liam Johnson (MBR-004): total member_resp = 150.00
('CLM-SEED-009', 'MBR-004', 'CLM-2025-10009', '2025-04-02', 'Pediatric Associates',      '1234567894', 'Z00.129','99393', 250.00,  200.00,   50.00,  150.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-04-03T08:00:00', '2025-04-03T08:00:00'),

-- David Chen (MBR-005): total member_resp = 4200.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-010', 'MBR-005', 'CLM-2025-10010', '2025-01-15', 'Summit Surgical Center',    '1234567895', 'K80.20',  '47562', 8500.00, 6800.00, 3800.00, 3000.00, 'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-01-16T08:00:00', '2025-01-16T08:00:00'),
('CLM-SEED-011', 'MBR-005', 'CLM-2025-10011', '2025-02-20', 'Summit Surgical Center',    '1234567895', 'K80.20',  '99213', 350.00,  280.00,   80.00,  200.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-02-21T08:00:00', '2025-02-21T08:00:00'),
('CLM-SEED-012', 'MBR-005', 'CLM-2025-10012', '2025-03-28', 'Cascade Medical Group',     '1234567890', 'K80.20',  '99214', 500.00,  400.00,  100.00,  300.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-03-29T08:00:00', '2025-03-29T08:00:00'),
('CLM-SEED-013', 'MBR-005', 'CLM-2025-10013', '2025-05-05', 'Regional Gastroenterology', '1234567896', 'K21.0',   '43239', 3200.00, 2560.00, 1860.00,  700.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-05-06T08:00:00', '2025-05-06T08:00:00');

-- Sarah Chen (MBR-006): total member_resp = 1100.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-014', 'MBR-006', 'CLM-2025-10014', '2025-02-10', 'Women''s Health Clinic',     '1234567893', 'Z01.419', '99395', 600.00,  480.00,  130.00,  350.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-02-11T08:00:00', '2025-02-11T08:00:00'),
('CLM-SEED-015', 'MBR-006', 'CLM-2025-10015', '2025-04-08', 'Cascade Medical Group',     '1234567890', 'R51.9',   '99213', 450.00,  360.00,  110.00,  250.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-04-09T08:00:00', '2025-04-09T08:00:00'),
('CLM-SEED-016', 'MBR-006', 'CLM-2025-10016', '2025-05-30', 'Summit Imaging Center',     '1234567897', 'R51.9',   '70553', 1800.00, 1440.00,  940.00,  500.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-05-31T08:00:00', '2025-05-31T08:00:00');

-- Olivia Chen (MBR-007): total member_resp = 450.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-017', 'MBR-007', 'CLM-2025-10017', '2025-03-22', 'Pediatric Associates',      '1234567894', 'J06.9',   '99213', 300.00,  240.00,   40.00,  200.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-03-23T08:00:00', '2025-03-23T08:00:00'),
('CLM-SEED-018', 'MBR-007', 'CLM-2025-10018', '2025-05-15', 'Pediatric Associates',      '1234567894', 'Z00.129', '99393', 250.00,  200.00,  -50.00,  250.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-05-16T08:00:00', '2025-05-16T08:00:00');

-- James Williams (MBR-008): total member_resp = 1800.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-019', 'MBR-008', 'CLM-2025-10019', '2025-01-28', 'Valley Orthopedics',        '1234567891', 'M17.11',  '99213', 450.00,  360.00,  110.00,  250.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-01-29T08:00:00', '2025-01-29T08:00:00'),
('CLM-SEED-020', 'MBR-008', 'CLM-2025-10020', '2025-02-25', 'Valley Orthopedics',        '1234567891', 'M17.11',  '27447', 12000.00, 9600.00, 7600.00, 1200.00, 'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-02-26T08:00:00', '2025-02-26T08:00:00'),
('CLM-SEED-021', 'MBR-008', 'CLM-2025-10021', '2025-04-15', 'Valley Orthopedics',        '1234567891', 'M17.11',  '99213', 450.00,  360.00,  10.00,   350.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-04-16T08:00:00', '2025-04-16T08:00:00');

-- Patricia Williams (MBR-009): total member_resp = 600.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-022', 'MBR-009', 'CLM-2025-10022', '2025-03-05', 'Cascade Medical Group',     '1234567890', 'E11.9',   '99214', 500.00,  400.00,  100.00,  300.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-03-06T08:00:00', '2025-03-06T08:00:00'),
('CLM-SEED-023', 'MBR-009', 'CLM-2025-10023', '2025-05-18', 'Regional Endocrinology',    '1234567898', 'E11.9',   '99214', 550.00,  440.00,  140.00,  300.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-05-19T08:00:00', '2025-05-19T08:00:00');

-- Noah Williams (MBR-010): total member_resp = 200.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-024', 'MBR-010', 'CLM-2025-10024', '2025-04-28', 'Pediatric Associates',      '1234567894', 'Z00.129', '99394', 250.00,  200.00,   0.00,   200.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-04-29T08:00:00', '2025-04-29T08:00:00');

-- Emily Davis (MBR-011): total member_resp = 3400.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-025', 'MBR-011', 'CLM-2025-10025', '2025-01-10', 'Cascade Medical Group',     '1234567890', 'M79.3',   '99213', 350.00,  280.00,   80.00,  200.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-01-11T08:00:00', '2025-01-11T08:00:00'),
('CLM-SEED-026', 'MBR-011', 'CLM-2025-10026', '2025-02-08', 'Summit Surgical Center',    '1234567895', 'N83.20',  '58661', 7500.00, 6000.00, 4700.00, 1300.00, 'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-02-09T08:00:00', '2025-02-09T08:00:00'),
('CLM-SEED-027', 'MBR-011', 'CLM-2025-10027', '2025-03-20', 'Cascade Medical Group',     '1234567890', 'N83.20',  '99213', 350.00,  280.00,   80.00,  200.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-03-21T08:00:00', '2025-03-21T08:00:00'),
('CLM-SEED-028', 'MBR-011', 'CLM-2025-10028', '2025-04-22', 'Physical Therapy Plus',     '1234567899', 'M54.5',   '97110', 200.00,  160.00,   10.00,  150.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-04-23T08:00:00', '2025-04-23T08:00:00'),
('CLM-SEED-029', 'MBR-011', 'CLM-2025-10029', '2025-05-01', 'Cascade Imaging Center',    '1234567892', 'M54.5',   '72148', 2200.00, 1760.00,  210.00, 1550.00, 'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-05-02T08:00:00', '2025-05-02T08:00:00');

-- Michael Brown (MBR-012): total member_resp = 950.00
INSERT OR IGNORE INTO claim_records (claim_record_id, member_id, claim_number, service_date, provider_name, provider_npi, diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status, adjudication_status, file_id, processing_run_id, created_at, updated_at) VALUES
('CLM-SEED-030', 'MBR-012', 'CLM-2025-10030', '2025-02-18', 'Cascade Medical Group',     '1234567890', 'I10',     '99214', 500.00,  400.00,   50.00,  350.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-002', 'RUN-SEED-002', '2025-02-19T08:00:00', '2025-02-19T08:00:00'),
('CLM-SEED-031', 'MBR-012', 'CLM-2025-10031', '2025-04-10', 'Regional Cardiology',       '1234567900', 'I10',     '93000', 800.00,  640.00,   40.00,  600.00,  'PROCESSED', 'ADJUDICATED', 'FIL-SEED-004', 'RUN-SEED-004', '2025-04-11T08:00:00', '2025-04-11T08:00:00');


-- ───────────────────────────────────────────────────────────────
-- VERIFICATION QUERIES (for manual validation — do not run in prod)
-- ───────────────────────────────────────────────────────────────
--
-- Verify claim totals match accumulator snapshots:
--
--   SELECT
--       m.member_id,
--       m.first_name || ' ' || m.last_name AS name,
--       a.accumulator_type,
--       a.current_amount AS accum_amt,
--       SUM(cr.member_responsibility) AS claim_total,
--       ROUND(a.current_amount - SUM(cr.member_responsibility), 2) AS drift
--   FROM members m
--   JOIN accumulator_snapshots a ON m.member_id = a.member_id
--       AND a.accumulator_type = 'oop_individual'
--   LEFT JOIN claim_records cr ON m.member_id = cr.member_id
--       AND cr.claim_status = 'PROCESSED'
--   GROUP BY m.member_id, m.first_name, m.last_name,
--            a.accumulator_type, a.current_amount
--   ORDER BY m.member_id;
--
-- Expected: All drift values = 0.00 for OOP individual accumulators.
--
-- Verify family rollup:
--
--   SELECT
--       m.family_id,
--       SUM(cr.member_responsibility) AS family_claim_total,
--       fa.current_amount AS family_accum
--   FROM members m
--   JOIN claim_records cr ON m.member_id = cr.member_id
--       AND cr.claim_status = 'PROCESSED'
--   JOIN accumulator_snapshots fa ON fa.member_id = (
--       SELECT member_id FROM members
--       WHERE family_id = m.family_id AND relationship_code = 'SUBSCRIBER'
--       LIMIT 1
--   ) AND fa.accumulator_type = 'oop_family'
--   WHERE m.family_id IN ('FAM-001', 'FAM-002', 'FAM-003')
--   GROUP BY m.family_id, fa.current_amount;
--
-- Expected: family_claim_total = family_accum for all three families.
-- ═══════════════════════════════════════════════════════════════