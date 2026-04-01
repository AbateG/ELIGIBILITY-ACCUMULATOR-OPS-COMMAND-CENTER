# Eligibility & Accumulator Operations Command Center

A portfolio-grade healthcare data operations simulator that models real-world
eligibility file processing, benefit accumulator tracking, incident triage,
SLA management, and root cause analysis — the daily operational reality of
healthcare data engineering and production support teams.

---

## Why This Exists

Every major health plan, PBM, and TPA runs nightly eligibility and accumulator
pipelines that determine whether a member can receive care and how much they owe.
When these pipelines break — a file arrives late, a member loads twice, a
deductible overstates — **real people are affected at the pharmacy counter and
the doctor's office.**

Production support teams that own these pipelines need:

- deterministic file monitoring and intake validation
- issue detection with severity-aware routing
- SLA-driven case management
- SQL-heavy root cause analysis
- accumulator reconciliation against plan thresholds
- runbook-driven remediation

This simulator recreates that entire operational surface — from raw file
generation through Streamlit-based investigation dashboards — so that the
skills can be demonstrated, tested, and packaged without requiring real PHI
or enterprise infrastructure.

---

## Ethical and Privacy Considerations

This project is designed as a **portfolio demonstration tool** using exclusively **synthetic data**. No real personally identifiable information (PII), protected health information (PHI), or sensitive data is used or simulated in any form.

- **Data Generation**: All member data, eligibility records, claims, and accumulator values are generated using the `faker` library with fictional entities (e.g., "ACME Health", "PAYERX").
- **No Real PHI**: Fictional names, addresses, phone numbers (555 area codes), emails with ".example.com" domains, and synthetic SSNs ensure compliance with privacy regulations.
- **Compliance**: The simulator adheres to ethical standards by avoiding any real-world data that could pose privacy risks, GDPR violations, or HIPAA concerns.
- **Educational Purpose**: Demonstrates healthcare data operations skills without requiring access to sensitive information, promoting responsible data handling practices.

By using this simulator, users can learn and showcase operational skills in a safe, privacy-conscious environment.

---

## What This Demonstrates

| Skill Area | How It Appears |
|---|---|
| Healthcare data pipelines | Eligibility and claims file generation, intake, validation, processing |
| Eligibility domain knowledge | Member/plan enrollment, coverage dates, client/vendor relationships |
| Accumulator logic | Deductible and OOP tracking, family rollup, plan threshold enforcement |
| Production support | Issue detection, support case creation, queue routing, SLA tracking |
| SQL-heavy root cause analysis | Targeted diagnostic queries per scenario with interpretation guidance |
| File monitoring | Inbound file registration, duplicate detection, missing file alerting |
| Incident triage | Severity-driven prioritization, at-risk/breached SLA escalation |
| Documentation as product | Runbooks, SQL playbooks, scenario catalog, architecture narrative |
| Continuous improvement | Deterministic scenario replay, before/after operational deltas |

---

## Core Scenarios

The simulator is organized around **5 deterministic support scenarios**, each
representing a real production incident pattern:

| # | Scenario Code | Business Meaning |
|---|---|---|
| 1 | `MISSING_INBOUND_FILE` | Expected eligibility file never arrived from vendor |
| 2 | `DUPLICATE_ELIGIBILITY_RESEND` | Vendor resent a file already processed, risking double-load |
| 3 | `CLAIM_INELIGIBLE_MEMBER` | Claim arrived for a member with no active eligibility |
| 4 | `ACCUMULATOR_EXCEEDS_OOP_MAX` | Member's accumulator balance exceeds their plan OOP maximum |
| 5 | `FAMILY_ROLLUP_DISCREPANCY` | Family-level accumulator total doesn't match sum of member transactions |

Each scenario generates traceable artifacts across every layer: files → issues →
support cases → SLA records → accumulator state — all queryable, all
investigable from the Streamlit UI.

See [`docs/scenario_catalog.md`](docs/scenario_catalog.md) for full details.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Streamlit Application                     │
│  ┌────────────┬───────────┬──────────────┬────────────────┐ │
│  │   Issue     │   File    │ Accumulator  │   Scenario     │ │
│  │  Triage     │ Monitoring│   Recon      │ Control Center │ │
│  └─────┬──────┴─────┬─────┴──────┬───────┴───────┬────────┘ │
│        │            │            │               │           │
│  ┌─────▼────────────▼────────────▼───────────────▼────────┐ │
│  │              SQLite Database (sim.db)                    │ │
│  │  members · plans · inbound_files · processing_runs      │ │
│  │  eligibility_records · claim_records                     │ │
│  │  accumulator_transactions · accumulator_snapshots        │ │
│  │  data_quality_issues · support_cases · sla_tracking      │ │
│  └─────▲────────────▲────────────▲───────────────▲────────┘ │
│        │            │            │               │           │
│  ┌─────┴──────┬─────┴─────┬──────┴───────┬───────┴────────┐ │
│  │   Data      │ Ingestion │ Validation   │  Accumulator   │ │
│  │ Generation  │ & Process │ & Issues     │  Engines       │ │
│  └────────────┴───────────┴──────────────┴────────────────┘ │
│                                                              │
│  ┌──────────────────────────────────────────────────────────┐│
│  │  Scenario Loaders (deterministic injection per scenario) ││
│  └──────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

**Stack:** Python · SQLite · Streamlit · pytest
**Cost:** $0 — no cloud services, no paid APIs, no licensed data

---

## Project Structure

```
eligibility-accumulator-ops/
├── config/
│   ├── settings.py                 # Central configuration
│   └── file_patterns.py            # Expected file naming conventions
├── src/
│   ├── common/
│   │   ├── db.py                   # DB connection management
│   │   ├── constants.py            # Domain constants
│   │   ├── file_utils.py           # File path helpers
│   │   └── datetime_utils.py       # Timestamp utilities
│   ├── db/
│   │   └── init_db.py              # Schema initialization
│   ├── data_generation/
│   │   ├── generate_seed_data.py         # Members, plans, clients, vendors
│   │   ├── generate_eligibility_files.py # Eligibility file simulation
│   │   └── generate_claim_files.py       # Claims file simulation
│   ├── ingestion/
│   │   └── register_inbound_files.py     # File intake and registration
│   ├── processing/
│   │   ├── process_eligibility.py        # Eligibility record loading
│   │   ├── post_load_eligibility_checks.py # Post-load validation
│   │   └── process_claims.py             # Claims processing
│   ├── validation/
│   │   ├── file_validators.py            # File-level checks
│   │   ├── eligibility_validators.py     # Eligibility business rules
│   │   ├── claims_validators.py          # Claims business rules
│   │   └── run_validations.py            # Validation orchestrator
│   ├── accumulators/
│   │   ├── transaction_engine.py         # Accumulator transaction processing
│   │   └── snapshot_engine.py            # Point-in-time snapshot generation
│   ├── issues/
│   │   ├── issue_service.py              # Data quality issue management
│   │   ├── support_case_service.py       # Support case creation/routing
│   │   └── run_case_generation.py        # Case generation orchestrator
│   ├── sla/
│   │   └── sla_service.py               # SLA tracking and evaluation
│   ├── scenarios/
│   │   ├── scenario_missing_inbound_file.py
│   │   ├── scenario_duplicate_eligibility_resend.py
│   │   ├── scenario_claim_for_ineligible_member.py
│   │   ├── scenario_accumulator_oop_exceeded.py
│   │   └── scenario_family_rollup_discrepancy.py
│   └── app/
│       ├── Home.py                       # Landing page
│       ├── utils.py                      # Streamlit helpers
│       └── pages/
│           ├── Issue_Triage.py
│           ├── File_Monitoring.py
│           ├── Accumulator_Reconciliation.py
│           ├── Scenario_Control_Center.py
│           ├── SQL_Query_Workbench.py
│           └── Investigation_Playbooks.py
├── tests/                                # 40+ passing tests
├── docs/
│   ├── scenario_catalog.md
│   ├── runbooks/
│   │   ├── runbook_missing_inbound_file.md
│   │   ├── runbook_duplicate_eligibility_resend.md
│   │   ├── runbook_claim_ineligible_member.md
│   │   ├── runbook_accumulator_oop_exceeded.md
│   │   └── runbook_family_rollup_discrepancy.md
│   └── sql_playbooks/
│       ├── sql_missing_inbound_file.md
│       ├── sql_duplicate_eligibility_resend.md
│       ├── sql_claim_ineligible_member.md
│       ├── sql_accumulator_oop_exceeded.md
│       └── sql_family_rollup_discrepancy.md
├── requirements.txt
└── README.md
```

---

## How to Run

### Prerequisites
- Python 3.9+
- pip

### Setup
```bash
git clone https://github.com/<your-handle>/eligibility-accumulator-ops.git
cd eligibility-accumulator-ops
pip install -r requirements.txt
pre-commit install
```

### Initialize and Seed
```bash
python -m src.db.init_db
python -m src.data_generation.generate_seed_data
```

### Run the Application
```bash
streamlit run src/app/Home.py
```

### Run Tests
```bash
pytest tests/ -v
```

### Run a Scenario
From the Streamlit **Scenario Control Center**, select any of the 5 scenarios,
trigger it, and then navigate to the linked investigation page to triage the
resulting issues, cases, and SLA records.

---

## How to Demo (7 Minutes)

0:00–0:45 — Home
Open on Home page. Show operational summary: KPI tiles, operational charts, activity feed, navigation guide.

0:45–1:30 — File Monitoring
Show exception worklist, missing file alert, file investigation details.

1:30–2:15 — Issue Triage
Show support queue, assignment group, priority/severity, SLA watchlist, case analytics.

2:15–3:00 — Accumulator Reconciliation
Show reconciliation worklist, member investigation, OOP progress bars.

3:00–3:50 — Scenario Control Center
Launch a scenario, show before/after deltas, run history.

3:50–4:50 — SQL Query Workbench
Show guided query, sandbox with safety controls, schema explorer.

4:50–5:50 — Investigation Playbooks
Open a playbook, show step-by-step SQL, decision tree, conclusion checklist.

5:50–6:30 — Cross-page workflow recap
Show how scenarios create issues, investigations link across pages.

6:30–7:00 — Close
Highlight healthcare domain, SQL investigation, operational workflows, deterministic simulation.

Total: ~7 minutes. Demonstrates full operational command center.

---

## Skills Demonstrated

- **Healthcare domain:** eligibility enrollment, benefit plans, deductibles,
  OOP maximums, family vs individual accumulation, client/vendor file operations
- **Data engineering:** file-based ingestion, validation pipelines, data quality
  issue detection, processing run tracking, accumulator state management
- **Production support:** incident triage, severity-based routing, SLA
  enforcement, root cause analysis, support case lifecycle
- **SQL:** diagnostic queries for every scenario, join-heavy investigation
  across files → issues → cases → accumulators → members → plans
- **Python:** modular service architecture, deterministic scenario injection,
  test coverage, Streamlit-based operational UI
- **Documentation:** runbooks, SQL playbooks, scenario catalog, architecture
  narrative — all written as operational artifacts, not afterthoughts

---

## Documentation

| Document | Purpose |
|---|---|
| [`docs/scenario_catalog.md`](docs/scenario_catalog.md) | All 5 scenarios with triggers, routing, SLAs, and investigation guidance |
| [`docs/runbooks/`](docs/runbooks/) | Step-by-step incident response per scenario |
| [`docs/sql_playbooks/`](docs/sql_playbooks/) | Diagnostic SQL queries per scenario with interpretation notes |

---

## Status

- **27+ tests passing** across accumulators, validators, DB init, support cases,
  SLA service, and all 5 scenario loaders
- **7 pages** in Streamlit: Home, Issue Triage, File Monitoring,
  Accumulator Reconciliation, Scenario Control Center, SQL Query Workbench, Investigation Playbooks
- **5 deterministic scenarios** with full artifact traceability
- **Zero external cost** — runs entirely on SQLite and local Python