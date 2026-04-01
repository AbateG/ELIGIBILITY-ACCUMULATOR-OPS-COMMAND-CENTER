import random
from pathlib import Path

import pandas as pd

from config.settings import DEFAULT_RANDOM_SEED, INBOUND_DIR
from src.common.db import fetch_all
from src.common.file_utils import ensure_directory

random.seed(DEFAULT_RANDOM_SEED)


def get_member_seed_base():
    query = """
    SELECT
        m.member_id,
        m.subscriber_id,
        m.relationship_code,
        m.first_name,
        m.last_name,
        m.dob,
        m.gender,
        c.client_code
    FROM members m
    JOIN clients c
      ON m.client_id = c.client_id
    ORDER BY m.member_id
    """
    return fetch_all(query)


def get_plan_codes():
    rows = fetch_all("SELECT plan_code FROM benefit_plans ORDER BY plan_id")
    return [r["plan_code"] for r in rows]


def build_clean_eligibility_dataframe(row_limit: int = 40) -> pd.DataFrame:
    members = get_member_seed_base()[:row_limit]
    plan_codes = get_plan_codes()
    records = []

    for idx, member in enumerate(members, start=1):
        records.append(
            {
                "record_id": f"ELIGREC{idx:05d}",
                "client_code": member["client_code"],
                "vendor_code": "TPA1",
                "subscriber_id": member["subscriber_id"],
                "member_id": member["member_id"],
                "relationship_code": member["relationship_code"],
                "first_name": member["first_name"],
                "last_name": member["last_name"],
                "dob": member["dob"],
                "gender": member["gender"],
                "plan_code": random.choice(plan_codes),
                "coverage_start": "2025-01-01",
                "coverage_end": "2025-12-31",
                "status": "ACTIVE",
                "group_id": f"{member['client_code']}_GRP_001",
            }
        )

    return pd.DataFrame(records)


def build_defective_eligibility_dataframe(clean_df: pd.DataFrame) -> pd.DataFrame:
    defective_df = clean_df.copy()

    # Duplicate a few rows
    dup_rows = defective_df.iloc[:3].copy()
    defective_df = pd.concat([defective_df, dup_rows], ignore_index=True)

    # Overlapping period for one member
    overlap_row = defective_df.iloc[0].copy()
    overlap_row["record_id"] = "ELIGREC_OVERLAP_0001"
    overlap_row["coverage_start"] = "2025-06-01"
    overlap_row["coverage_end"] = "2025-12-31"
    defective_df = pd.concat([defective_df, pd.DataFrame([overlap_row])], ignore_index=True)

    # Null plan code
    defective_df.loc[1, "plan_code"] = None

    return defective_df


def generate_eligibility_files() -> None:
    ensure_directory(INBOUND_DIR)

    clean_df = build_clean_eligibility_dataframe()
    defective_df = build_defective_eligibility_dataframe(clean_df)

    clean_path = INBOUND_DIR / "ELIG_ACME_TPA1_20250101.csv"
    defective_path = INBOUND_DIR / "ELIG_ACME_TPA1_20250102_DUP.csv"

    clean_df.to_csv(clean_path, index=False)
    defective_df.to_csv(defective_path, index=False)

    print(f"Created clean eligibility file: {clean_path}")
    print(f"Created defective eligibility file: {defective_path}")


if __name__ == "__main__":
    generate_eligibility_files()