eligibility-accumulator-ops-command-center/
│
├── src/
│   ├── app/
│   │   ├── Home.py
│   │   ├── shared_ui.py
│   │   └── pages/
│   │       ├── Accumulator_Reconciliation.py
│   │       ├── File_Detail.py
│   │       ├── File_Monitoring.py
│   │       ├── Issue_Triage.py
│   │       ├── Member_Timeline.py
│   │       ├── Processing_Run_Dashboard.py
│   │       ├── Processing_Run_Detail.py
│   │       ├── SLA_Detail.py
│   │       ├── Scenario_Control_Center.py
│   │       ├── Support_Case_Detail.py
│   │       └── ... (12 total pages)
│   ├── common/
│   │   ├── constants.py
│   │   ├── datetime_utils.py
│   │   ├── db.py
│   │   ├── file_utils.py
│   │   └── observability.py
│   ├── db/
│   │   └── init_db.py
│   ├── processing/
│   │   ├── post_load_eligibility_checks.py
│   │   ├── process_claims.py
│   │   ├── process_eligibility.py
│   │   └── processing_helpers.py
│   ├── accumulators/
│   │   ├── snapshot_engine.py
│   │   └── transaction_engine.py
│   ├── validation/
│   │   ├── _common.py
│   │   ├── claims_validators.py
│   │   ├── eligibility_validators.py
│   │   ├── file_validators.py
│   │   └── run_validations.py
│   ├── issues/
│   │   └── support_case_service.py
│   ├── sla/
│   │   └── sla_service.py
│   ├── ingestion/
│   │   └── register_inbound_files.py
│   └── scenarios/
│       ├── scenario_accumulator_oop_exceeded.py
│       ├── scenario_family_rollup_discrepancy.py
│       └── scenario_missing_inbound_file.py
│
├── db/
│   ├── schema.sql (legacy, init_db.py is source of truth)
│   └── queries/ (empty, SQL playbooks in docs/)
│
├── docs/
│   ├── runbooks/
│   │   └── ... (moved from root runbooks/)
│   ├── sql_playbooks/
│   │   └── ... (diagnostic queries)
│   ├── PROD_READINESS.md
│   ├── architecture.md
│   └── ...
│
├── tests/
│   ├── test_*.py (27+ test files)
│   └── ...
│
├── config/
│   ├── settings.py
│   └── file_patterns.py
│
├── runbooks/ (deprecated, moved to docs/runbooks/)
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
└── README.md
│
├── README.md
└── requirements.txt