import os
import random
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

from config.settings import DB_PATH, DEFAULT_BENEFIT_YEAR, DEFAULT_RANDOM_SEED
from src.common.db import db_session

fake = Faker()
random.seed(DEFAULT_RANDOM_SEED)
Faker.seed(DEFAULT_RANDOM_SEED)


CLIENTS = [
    ("ACME", "Acme Manufacturing"),
    ("BETA", "Beta Logistics"),
]

VENDORS = [
    ("TPA1", "Third Party Admin One", "TPA"),
    ("PAYERX", "Payer Exchange", "PAYER"),
    ("VENDNET", "Vendor Network", "TRADING_PARTNER"),
]

PLANS = [
    (
        "PPO_STD_2025",
        "PPO Standard 2025",
        "PPO",
        DEFAULT_BENEFIT_YEAR,
        1500.00,
        3000.00,
        5000.00,
        10000.00,
        0.20,
        30.00,
        60.00,
        1,
        "EMBEDDED",
    ),
    (
        "HDHP_2025",
        "HDHP 2025",
        "HDHP",
        DEFAULT_BENEFIT_YEAR,
        3200.00,
        6400.00,
        6000.00,
        12000.00,
        0.10,
        0.00,
        0.00,
        1,
        "AGGREGATE",
    ),
    (
        "EPO_RICH_2025",
        "EPO Rich 2025",
        "EPO",
        DEFAULT_BENEFIT_YEAR,
        750.00,
        1500.00,
        3500.00,
        7000.00,
        0.15,
        20.00,
        40.00,
        1,
        "EMBEDDED",
    ),
]


def generate_member_rows(num_subscribers: int = 20):
    rows = []
    client_ids = [1, 2]
    relationship_options = [
        ["SUB"],
        ["SUB", "SPOUSE"],
        ["SUB", "CHILD"],
        ["SUB", "CHILD", "CHILD"],
        ["SUB", "SPOUSE", "CHILD"],
        ["SUB", "SPOUSE", "CHILD", "CHILD"],
    ]

    for i in range(1, num_subscribers + 1):
        subscriber_id = f"S{i:04d}"
        family_id = f"F{i:04d}"
        client_id = random.choice(client_ids)
        family_structure = random.choice(relationship_options)

        for j, rel in enumerate(family_structure, start=1):
            member_id = f"{subscriber_id}-M{j}"
            first_name = fake.first_name()
            last_name = fake.last_name()
            dob = fake.date_of_birth(minimum_age=1, maximum_age=64).isoformat()
            gender = random.choice(["M", "F"])

            rows.append(
                (
                    member_id,
                    subscriber_id,
                    client_id,
                    first_name,
                    last_name,
                    dob,
                    gender,
                    rel,
                    family_id,
                )
            )
    return rows


def seed_reference_data() -> None:
    db_path = Path(os.getenv("SQLITE_DB_PATH", str(DB_PATH)))
    with db_session(db_path) as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO clients (client_code, client_name)
            VALUES (?, ?)
            """,
            CLIENTS,
        )

        conn.executemany(
            """
            INSERT OR IGNORE INTO vendors (vendor_code, vendor_name, vendor_type)
            VALUES (?, ?, ?)
            """,
            VENDORS,
        )

        conn.executemany(
            """
            INSERT OR IGNORE INTO benefit_plans (
                plan_code, plan_name, plan_type, benefit_year,
                individual_deductible, family_deductible,
                individual_oop_max, family_oop_max,
                coinsurance_rate, primary_copay, specialist_copay,
                preventive_exempt_flag, family_accumulation_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            PLANS,
        )

        member_rows = generate_member_rows()
        conn.executemany(
            """
            INSERT OR IGNORE INTO members (
                member_id, subscriber_id, client_id, first_name, last_name,
                dob, gender, relationship_code, family_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            member_rows,
        )

        conn.execute(
            """
            INSERT INTO audit_log (event_type, entity_name, entity_key, actor, event_details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "SEED_DATA",
                "reference_data",
                "initial_seed",
                "system",
                f"Seeded {len(CLIENTS)} clients, {len(VENDORS)} vendors, {len(PLANS)} plans, {len(member_rows)} members",
            ),
        )

    print("Seed data generated successfully.")


if __name__ == "__main__":
    seed_reference_data()