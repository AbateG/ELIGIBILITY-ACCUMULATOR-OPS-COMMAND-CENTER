PRAGMA foreign_keys = ON;

-- =========================================================
-- SCHEMA METADATA
-- =========================================================
CREATE TABLE IF NOT EXISTS schema_metadata (
    schema_version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT
);

-- =========================================================
-- MASTER / REFERENCE TABLES
-- =========================================================
CREATE TABLE IF NOT EXISTS clients (
    client_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_code TEXT NOT NULL UNIQUE,
    client_name TEXT NOT NULL,
    active_flag INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vendors (
    vendor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_code TEXT NOT NULL UNIQUE,
    vendor_name TEXT NOT NULL,
    vendor_type TEXT NOT NULL,
    active_flag INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS client_vendor_relationships (
    relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    vendor_id INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
);

CREATE TABLE IF NOT EXISTS vendor_contacts (
    contact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id INTEGER NOT NULL,
    contact_name TEXT NOT NULL,
    contact_email TEXT,
    contact_phone TEXT,
    contact_type TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
);

CREATE TABLE IF NOT EXISTS benefit_plans (
    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_code TEXT NOT NULL UNIQUE,
    plan_name TEXT NOT NULL,
    plan_type TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    benefit_year INTEGER NOT NULL,
    individual_deductible REAL NOT NULL,
    family_deductible REAL NOT NULL,
    individual_oop_max REAL NOT NULL,
    family_oop_max REAL NOT NULL,
    coinsurance_rate REAL NOT NULL DEFAULT 0.20,
    primary_copay REAL NOT NULL DEFAULT 0,
    specialist_copay REAL NOT NULL DEFAULT 0,
    preventive_exempt_flag INTEGER NOT NULL DEFAULT 1,
    family_accumulation_type TEXT NOT NULL DEFAULT 'EMBEDDED',
    active_flag INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id)
);

CREATE TABLE IF NOT EXISTS members (
    member_id TEXT PRIMARY KEY,
    subscriber_id TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    dob TEXT NOT NULL,
    gender TEXT,
    relationship_code TEXT NOT NULL,
    family_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id)
);

-- =========================================================
-- FILE REGISTRY (must come before tables that reference it)
-- =========================================================
CREATE TABLE IF NOT EXISTS inbound_files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT NULL UNIQUE,
    file_type TEXT NOT NULL,
    client_id INTEGER,
    vendor_id INTEGER,
    expected_date TEXT,
    received_ts TEXT,
    file_hash TEXT,
    row_count INTEGER,
    processing_status TEXT NOT NULL DEFAULT 'RECEIVED',
    duplicate_flag INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    landing_path TEXT,
    archived_path TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
);

CREATE TABLE IF NOT EXISTS outbound_files (
    outbound_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT NULL UNIQUE,
    file_type TEXT NOT NULL,
    client_id INTEGER,
    vendor_id INTEGER,
    generated_ts TEXT,
    row_count INTEGER,
    processing_status TEXT NOT NULL DEFAULT 'GENERATED',
    output_path TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
);

CREATE TABLE IF NOT EXISTS file_schedules (
    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    vendor_id INTEGER NOT NULL,
    file_type TEXT NOT NULL,
    file_direction TEXT NOT NULL DEFAULT 'INBOUND',
    frequency TEXT NOT NULL DEFAULT 'DAILY',
    expected_time TEXT,
    day_of_week TEXT,
    day_of_month INTEGER,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
);

-- =========================================================
-- ELIGIBILITY
-- =========================================================
CREATE TABLE IF NOT EXISTS eligibility_periods (
    eligibility_id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    vendor_id INTEGER,
    group_id TEXT,
    coverage_start TEXT NOT NULL,
    coverage_end TEXT,
    status TEXT NOT NULL,
    source_file_id INTEGER,
    source_row_number INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES members(member_id),
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (plan_id) REFERENCES benefit_plans(plan_id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
    FOREIGN KEY (source_file_id) REFERENCES inbound_files(file_id)
);

-- =========================================================
-- CLAIMS
-- =========================================================
CREATE TABLE IF NOT EXISTS claims (
    claim_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id TEXT NOT NULL,
    line_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    vendor_id INTEGER,
    service_date TEXT NOT NULL,
    paid_date TEXT,
    allowed_amount REAL NOT NULL,
    paid_amount REAL NOT NULL,
    member_responsibility REAL NOT NULL,
    deductible_amount REAL NOT NULL DEFAULT 0,
    coinsurance_amount REAL NOT NULL DEFAULT 0,
    copay_amount REAL NOT NULL DEFAULT 0,
    preventive_flag INTEGER NOT NULL DEFAULT 0,
    reversal_flag INTEGER NOT NULL DEFAULT 0,
    claim_status TEXT NOT NULL,
    source_file_id INTEGER,
    source_row_number INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES members(member_id),
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (plan_id) REFERENCES benefit_plans(plan_id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
    FOREIGN KEY (source_file_id) REFERENCES inbound_files(file_id),
    UNIQUE (claim_id, line_id)
);

-- =========================================================
-- ACCUMULATOR TRANSACTIONS
-- =========================================================
CREATE TABLE IF NOT EXISTS accumulator_transactions (
    accumulator_txn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id TEXT NOT NULL,
    family_id TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    claim_record_id INTEGER,
    benefit_year INTEGER NOT NULL,
    accumulator_type TEXT NOT NULL,
    delta_amount REAL NOT NULL,
    service_date TEXT NOT NULL,
    source_type TEXT NOT NULL DEFAULT 'CLAIM',
    source_file_id INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (member_id) REFERENCES members(member_id),
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (plan_id) REFERENCES benefit_plans(plan_id),
    FOREIGN KEY (claim_record_id) REFERENCES claims(claim_record_id),
    FOREIGN KEY (source_file_id) REFERENCES inbound_files(file_id)
);

-- =========================================================
-- ACCUMULATOR SNAPSHOTS
-- =========================================================
CREATE TABLE IF NOT EXISTS accumulator_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id TEXT NOT NULL,
    family_id TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    benefit_year INTEGER NOT NULL,
    individual_deductible_accum REAL NOT NULL DEFAULT 0,
    family_deductible_accum REAL NOT NULL DEFAULT 0,
    individual_oop_accum REAL NOT NULL DEFAULT 0,
    family_oop_accum REAL NOT NULL DEFAULT 0,
    individual_deductible_met_flag INTEGER NOT NULL DEFAULT 0,
    family_deductible_met_flag INTEGER NOT NULL DEFAULT 0,
    individual_oop_met_flag INTEGER NOT NULL DEFAULT 0,
    family_oop_met_flag INTEGER NOT NULL DEFAULT 0,
    snapshot_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (member_id, plan_id, benefit_year),
    FOREIGN KEY (member_id) REFERENCES members(member_id),
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (plan_id) REFERENCES benefit_plans(plan_id)
);

-- =========================================================
-- PROCESSING RUNS
-- =========================================================
CREATE TABLE IF NOT EXISTS processing_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type TEXT NOT NULL,
    file_id INTEGER,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    run_status TEXT NOT NULL,
    rows_read INTEGER DEFAULT 0,
    rows_passed INTEGER DEFAULT 0,
    rows_failed INTEGER DEFAULT 0,
    issue_count INTEGER DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (file_id) REFERENCES inbound_files(file_id)
);

-- =========================================================
-- DATA QUALITY ISSUES
-- =========================================================
CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_code TEXT,
    issue_type TEXT NOT NULL,
    issue_subtype TEXT,
    severity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    client_id INTEGER,
    vendor_id INTEGER,
    file_id INTEGER,
    run_id INTEGER,
    member_id TEXT,
    claim_record_id INTEGER,
    entity_name TEXT,
    entity_key TEXT,
    source_row_number INTEGER,
    issue_message TEXT,
    issue_description TEXT NOT NULL,
    detected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT,
    resolution_notes TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
    FOREIGN KEY (file_id) REFERENCES inbound_files(file_id),
    FOREIGN KEY (run_id) REFERENCES processing_runs(run_id),
    FOREIGN KEY (member_id) REFERENCES members(member_id),
    FOREIGN KEY (claim_record_id) REFERENCES claims(claim_record_id)
);

-- =========================================================
-- SUPPORT CASES
-- =========================================================
CREATE TABLE IF NOT EXISTS support_cases (
    case_id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_number TEXT NOT NULL UNIQUE,
    issue_id INTEGER,
    client_id INTEGER,
    vendor_id INTEGER,
    file_id INTEGER,
    run_id INTEGER,
    member_id TEXT,
    claim_record_id INTEGER,
    case_type TEXT NOT NULL,
    priority TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    assigned_team TEXT,
    assigned_to TEXT,
    short_description TEXT NOT NULL,
    description TEXT,
    root_cause_category TEXT,
    resolution_summary TEXT,
    escalation_level INTEGER NOT NULL DEFAULT 0,
    source_system TEXT NOT NULL DEFAULT 'SYSTEM',
    opened_at TEXT NOT NULL,
    acknowledged_at TEXT,
    resolved_at TEXT,
    closed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (issue_id) REFERENCES data_quality_issues(issue_id)
);

-- =========================================================
-- CASE NOTES
-- =========================================================
CREATE TABLE IF NOT EXISTS case_notes (
    note_id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL,
    note TEXT NOT NULL,
    author TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (case_id) REFERENCES support_cases(case_id)
);

-- =========================================================
-- AUDIT LOG
-- =========================================================
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type TEXT NOT NULL,
    entity_name TEXT,
    entity_key TEXT,
    run_id INTEGER,
    file_id INTEGER,
    actor TEXT NOT NULL DEFAULT 'system',
    event_details TEXT,
    FOREIGN KEY (run_id) REFERENCES processing_runs(run_id),
    FOREIGN KEY (file_id) REFERENCES inbound_files(file_id)
);

-- =========================================================
-- SLA TRACKING
-- =========================================================
CREATE TABLE IF NOT EXISTS sla_tracking (
    sla_id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL,
    sla_type TEXT NOT NULL,
    target_hours INTEGER,
    target_due_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    is_at_risk INTEGER NOT NULL DEFAULT 0,
    is_breached INTEGER NOT NULL DEFAULT 0,
    breached_at TEXT,
    last_evaluated_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (case_id) REFERENCES support_cases(case_id)
);

-- =========================================================
-- INDEXES
-- =========================================================
CREATE INDEX IF NOT EXISTS idx_members_client_id ON members (client_id);
CREATE INDEX IF NOT EXISTS idx_members_subscriber_id ON members (subscriber_id);
CREATE INDEX IF NOT EXISTS idx_members_family_id ON members (family_id);
CREATE INDEX IF NOT EXISTS idx_eligibility_member_id ON eligibility_periods (member_id);
CREATE INDEX IF NOT EXISTS idx_eligibility_plan_id ON eligibility_periods (plan_id);
CREATE INDEX IF NOT EXISTS idx_eligibility_coverage_dates ON eligibility_periods (coverage_start, coverage_end);
CREATE INDEX IF NOT EXISTS idx_claims_member_id ON claims (member_id);
CREATE INDEX IF NOT EXISTS idx_claims_service_date ON claims (service_date);
CREATE INDEX IF NOT EXISTS idx_claims_plan_id ON claims (plan_id);
CREATE INDEX IF NOT EXISTS idx_claims_source_file_id ON claims (source_file_id);
CREATE INDEX IF NOT EXISTS idx_acc_txn_member_year ON accumulator_transactions (member_id, benefit_year);
CREATE INDEX IF NOT EXISTS idx_acc_txn_family_year ON accumulator_transactions (family_id, benefit_year);
CREATE INDEX IF NOT EXISTS idx_acc_txn_claim_record_id ON accumulator_transactions (claim_record_id);
CREATE INDEX IF NOT EXISTS idx_acc_snap_member_year ON accumulator_snapshots (member_id, benefit_year);
CREATE INDEX IF NOT EXISTS idx_inbound_files_type_status ON inbound_files (file_type, processing_status);
CREATE INDEX IF NOT EXISTS idx_inbound_files_expected_date ON inbound_files (expected_date);
CREATE INDEX IF NOT EXISTS idx_processing_runs_file_id ON processing_runs (file_id);
CREATE INDEX IF NOT EXISTS idx_processing_runs_status ON processing_runs (run_status);
CREATE INDEX IF NOT EXISTS idx_dq_issues_status ON data_quality_issues (status);
CREATE INDEX IF NOT EXISTS idx_dq_issues_severity ON data_quality_issues (severity);
CREATE INDEX IF NOT EXISTS idx_dq_issues_file_id ON data_quality_issues (file_id);
CREATE INDEX IF NOT EXISTS idx_dq_issues_member_id ON data_quality_issues (member_id);
CREATE INDEX IF NOT EXISTS idx_dq_issues_issue_code ON data_quality_issues (issue_code);
CREATE INDEX IF NOT EXISTS idx_support_cases_status ON support_cases (status);
CREATE INDEX IF NOT EXISTS idx_support_cases_issue_id ON support_cases (issue_id);
CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_log (event_type);
CREATE INDEX IF NOT EXISTS idx_audit_run_id ON audit_log (run_id);

-- =========================================================
-- SEED SCHEMA METADATA
-- =========================================================
INSERT OR IGNORE INTO schema_metadata (schema_version, applied_at, description)
VALUES ('v2_unified', CURRENT_TIMESTAMP, 'Unified schema — matches app queries and seed data');