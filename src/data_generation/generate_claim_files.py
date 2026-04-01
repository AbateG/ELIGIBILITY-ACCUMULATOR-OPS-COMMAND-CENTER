import random

import pandas as pd

from config.settings import DEFAULT_RANDOM_SEED, INBOUND_DIR
from src.common.db import fetch_all
from src.common.file_utils import ensure_directory

random.seed(DEFAULT_RANDOM_SEED)


def get_eligible_member_plan_base(limit: int = 30):
    return fetch_all(
        f"""
        SELECT
            m.member_id,
            m.subscriber_id,
            m.family_id,
            c.client_code,
            p.plan_code,
            ep.coverage_start,
            COALESCE(ep.coverage_end, '2025-12-31') AS coverage_end
        FROM eligibility_periods ep
        JOIN members m
          ON ep.member_id = m.member_id
        JOIN clients c
          ON ep.client_id = c.client_id
        JOIN benefit_plans p
          ON ep.plan_id = p.plan_id
        ORDER BY m.member_id
        LIMIT {limit}
        """
    )


def build_clean_claims_dataframe(row_limit: int = 25) -> pd.DataFrame:
    members = get_eligible_member_plan_base(limit=row_limit)
    records = []

    for idx, member in enumerate(members, start=1):
        preventive_flag = 1 if idx % 7 == 0 else 0
        deductible_amount = 0.0 if preventive_flag else random.choice([0.0, 50.0, 100.0, 250.0])
        coinsurance_amount = random.choice([0.0, 20.0, 40.0, 75.0])
        copay_amount = random.choice([0.0, 20.0, 30.0, 40.0])
        member_responsibility = deductible_amount + coinsurance_amount + copay_amount
        allowed_amount = round(member_responsibility + random.choice([100.0, 150.0, 200.0, 300.0]), 2)
        paid_amount = round(allowed_amount - member_responsibility, 2)

        records.append(
            {
                "claim_id": f"CLM{idx:05d}",
                "line_id": "1",
                "client_code": member["client_code"],
                "vendor_code": "PAYERX",
                "member_id": member["member_id"],
                "subscriber_id": member["subscriber_id"],
                "plan_code": member["plan_code"],
                "service_date": "2025-03-15",
                "paid_date": "2025-03-20",
                "allowed_amount": allowed_amount,
                "paid_amount": paid_amount,
                "member_responsibility": member_responsibility,
                "deductible_amount": deductible_amount,
                "coinsurance_amount": coinsurance_amount,
                "copay_amount": copay_amount,
                "preventive_flag": preventive_flag,
                "claim_status": "PAID",
            }
        )

    return pd.DataFrame(records)


def build_defective_claims_dataframe(clean_df: pd.DataFrame) -> pd.DataFrame:
    defective_df = clean_df.copy()

    # 1. Unknown member
    defective_df.loc[0, "claim_id"] = "CLM_ERR_0001"
    defective_df.loc[0, "member_id"] = "UNKNOWN_MEMBER_999"
    defective_df.loc[0, "subscriber_id"] = "UNKNOWN_SUB_999"

    # 2. Ineligible service date outside coverage
    defective_df.loc[1, "claim_id"] = "CLM_ERR_0002"
    defective_df.loc[1, "service_date"] = "2024-12-15"

    # 3. Invalid numeric field
    defective_df.loc[2, "claim_id"] = "CLM_ERR_0003"
    defective_df.loc[2, "deductible_amount"] = "BAD_NUMERIC"

    # 4. Negative amount not marked reversal
    defective_df.loc[3, "claim_id"] = "CLM_ERR_0004"
    defective_df.loc[3, "deductible_amount"] = -50.0
    defective_df.loc[3, "claim_status"] = "PAID"

    # 5. OOP exceed candidate
    defective_df.loc[4, "claim_id"] = "CLM_ERR_0005"
    defective_df.loc[4, "deductible_amount"] = 3000.0
    defective_df.loc[4, "coinsurance_amount"] = 1500.0
    defective_df.loc[4, "copay_amount"] = 800.0
    defective_df.loc[4, "member_responsibility"] = 5300.0
    defective_df.loc[4, "allowed_amount"] = 8000.0
    defective_df.loc[4, "paid_amount"] = 2700.0

    return defective_df


def generate_claim_files() -> None:
    ensure_directory(INBOUND_DIR)

    clean_df = build_clean_claims_dataframe()
    defective_df = build_defective_claims_dataframe(clean_df)

    clean_path = INBOUND_DIR / "CLAIMS_ACME_PAYERX_20250103.csv"
    defective_path = INBOUND_DIR / "CLAIMS_ACME_PAYERX_20250104_ERR.csv"

    clean_df.to_csv(clean_path, index=False)
    defective_df.to_csv(defective_path, index=False)

    print(f"Created clean claims file: {clean_path}")
    print(f"Created defective claims file: {defective_path}")


if __name__ == "__main__":
    generate_claim_files()