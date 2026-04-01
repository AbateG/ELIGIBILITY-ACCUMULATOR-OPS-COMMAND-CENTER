def derive_accumulator_transactions(claim_row: dict, plan_row: dict, member_row: dict) -> list[dict]:
    transactions = []

    benefit_year = plan_row["benefit_year"]
    preventive_exempt = bool(plan_row["preventive_exempt_flag"])
    preventive_flag = bool(claim_row["preventive_flag"])

    deductible_amount = float(claim_row["deductible_amount"])
    coinsurance_amount = float(claim_row["coinsurance_amount"])
    copay_amount = float(claim_row["copay_amount"])

    if preventive_exempt and preventive_flag:
        deductible_contribution = 0.0
    else:
        deductible_contribution = deductible_amount

    oop_contribution = deductible_contribution + coinsurance_amount + copay_amount

    family_id = member_row["family_id"]

    if deductible_contribution != 0:
        transactions.append(
            {
                "member_id": claim_row["member_id"],
                "family_id": family_id,
                "client_id": claim_row["client_id"],
                "plan_id": claim_row["plan_id"],
                "claim_record_id": claim_row["claim_record_id"],
                "benefit_year": benefit_year,
                "accumulator_type": "IND_DED",
                "delta_amount": deductible_contribution,
                "service_date": claim_row["service_date"],
                "source_file_id": claim_row["source_file_id"],
            }
        )
        transactions.append(
            {
                "member_id": claim_row["member_id"],
                "family_id": family_id,
                "client_id": claim_row["client_id"],
                "plan_id": claim_row["plan_id"],
                "claim_record_id": claim_row["claim_record_id"],
                "benefit_year": benefit_year,
                "accumulator_type": "FAM_DED",
                "delta_amount": deductible_contribution,
                "service_date": claim_row["service_date"],
                "source_file_id": claim_row["source_file_id"],
            }
        )

    if oop_contribution != 0:
        transactions.append(
            {
                "member_id": claim_row["member_id"],
                "family_id": family_id,
                "client_id": claim_row["client_id"],
                "plan_id": claim_row["plan_id"],
                "claim_record_id": claim_row["claim_record_id"],
                "benefit_year": benefit_year,
                "accumulator_type": "IND_OOP",
                "delta_amount": oop_contribution,
                "service_date": claim_row["service_date"],
                "source_file_id": claim_row["source_file_id"],
            }
        )
        transactions.append(
            {
                "member_id": claim_row["member_id"],
                "family_id": family_id,
                "client_id": claim_row["client_id"],
                "plan_id": claim_row["plan_id"],
                "claim_record_id": claim_row["claim_record_id"],
                "benefit_year": benefit_year,
                "accumulator_type": "FAM_OOP",
                "delta_amount": oop_contribution,
                "service_date": claim_row["service_date"],
                "source_file_id": claim_row["source_file_id"],
            }
        )

    return transactions