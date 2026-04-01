from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db"
DATA_DIR = BASE_DIR / "data"
LANDING_DIR = DATA_DIR / "landing"
INBOUND_DIR = LANDING_DIR / "inbound"
ARCHIVE_DIR = LANDING_DIR / "archive"

DB_PATH = DB_DIR / "ops_simulator.db"
SCHEMA_PATH = DB_DIR / "schema.sql"

DEFAULT_BENEFIT_YEAR = 2025
DEFAULT_RANDOM_SEED = 42

SUPPORTED_FILE_TYPES = {"ELIGIBILITY", "CLAIMS"}

REQUIRED_ELIGIBILITY_COLUMNS = [
    "record_id",
    "client_code",
    "vendor_code",
    "subscriber_id",
    "member_id",
    "relationship_code",
    "first_name",
    "last_name",
    "dob",
    "gender",
    "plan_code",
    "coverage_start",
    "coverage_end",
    "status",
    "group_id",
]

REQUIRED_CLAIMS_COLUMNS = [
    "claim_id",
    "line_id",
    "client_code",
    "vendor_code",
    "member_id",
    "subscriber_id",
    "plan_code",
    "service_date",
    "paid_date",
    "allowed_amount",
    "paid_amount",
    "member_responsibility",
    "deductible_amount",
    "coinsurance_amount",
    "copay_amount",
    "preventive_flag",
    "claim_status",
]

