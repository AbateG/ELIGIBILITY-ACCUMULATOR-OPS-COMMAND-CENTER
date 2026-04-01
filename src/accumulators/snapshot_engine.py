from src.issues.issue_service import create_issue


def rebuild_accumulator_snapshots(conn, source_file_id: int | None = None) -> int:
    if source_file_id is not None:
        claim_filter = """
        WHERE at.source_file_id = ?
        """
        rows = conn.execute(
            f"""
            SELECT
                at.member_id,
                at.family_id,
                at.client_id,
                at.plan_id,
                at.benefit_year,
                SUM(CASE WHEN at.accumulator_type = 'IND_DED' THEN at.delta_amount ELSE 0 END) AS individual_deductible_accum,
                SUM(CASE WHEN at.accumulator_type = 'FAM_DED' THEN at.delta_amount ELSE 0 END) AS family_deductible_accum,
                SUM(CASE WHEN at.accumulator_type = 'IND_OOP' THEN at.delta_amount ELSE 0 END) AS individual_oop_accum,
                SUM(CASE WHEN at.accumulator_type = 'FAM_OOP' THEN at.delta_amount ELSE 0 END) AS family_oop_accum
            FROM accumulator_transactions at
            {claim_filter}
            GROUP BY at.member_id, at.family_id, at.client_id, at.plan_id, at.benefit_year
            """,
            (source_file_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                at.member_id,
                at.family_id,
                at.client_id,
                at.plan_id,
                at.benefit_year,
                SUM(CASE WHEN at.accumulator_type = 'IND_DED' THEN at.delta_amount ELSE 0 END) AS individual_deductible_accum,
                SUM(CASE WHEN at.accumulator_type = 'FAM_DED' THEN at.delta_amount ELSE 0 END) AS family_deductible_accum,
                SUM(CASE WHEN at.accumulator_type = 'IND_OOP' THEN at.delta_amount ELSE 0 END) AS individual_oop_accum,
                SUM(CASE WHEN at.accumulator_type = 'FAM_OOP' THEN at.delta_amount ELSE 0 END) AS family_oop_accum
            FROM accumulator_transactions at
            GROUP BY at.member_id, at.family_id, at.client_id, at.plan_id, at.benefit_year
            """
        ).fetchall()

    upsert_count = 0

    for row in rows:
        plan_row = conn.execute(
            """
            SELECT
                individual_deductible,
                family_deductible,
                individual_oop_max,
                family_oop_max
            FROM benefit_plans
            WHERE plan_id = ?
            """,
            (row["plan_id"],),
        ).fetchone()

        ind_ded = float(row["individual_deductible_accum"] or 0)
        fam_ded = float(row["family_deductible_accum"] or 0)
        ind_oop = float(row["individual_oop_accum"] or 0)
        fam_oop = float(row["family_oop_accum"] or 0)

        ind_ded_met = 1 if ind_ded >= float(plan_row["individual_deductible"]) else 0
        fam_ded_met = 1 if fam_ded >= float(plan_row["family_deductible"]) else 0
        ind_oop_met = 1 if ind_oop >= float(plan_row["individual_oop_max"]) else 0
        fam_oop_met = 1 if fam_oop >= float(plan_row["family_oop_max"]) else 0

        conn.execute(
            """
            INSERT INTO accumulator_snapshots (
                member_id, family_id, client_id, plan_id, benefit_year,
                individual_deductible_accum, family_deductible_accum,
                individual_oop_accum, family_oop_accum,
                individual_deductible_met_flag, family_deductible_met_flag,
                individual_oop_met_flag, family_oop_met_flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(member_id, plan_id, benefit_year)
            DO UPDATE SET
                family_id = excluded.family_id,
                client_id = excluded.client_id,
                individual_deductible_accum = excluded.individual_deductible_accum,
                family_deductible_accum = excluded.family_deductible_accum,
                individual_oop_accum = excluded.individual_oop_accum,
                family_oop_accum = excluded.family_oop_accum,
                individual_deductible_met_flag = excluded.individual_deductible_met_flag,
                family_deductible_met_flag = excluded.family_deductible_met_flag,
                individual_oop_met_flag = excluded.individual_oop_met_flag,
                family_oop_met_flag = excluded.family_oop_met_flag,
                snapshot_ts = CURRENT_TIMESTAMP
            """,
            (
                row["member_id"],
                row["family_id"],
                row["client_id"],
                row["plan_id"],
                row["benefit_year"],
                ind_ded,
                fam_ded,
                ind_oop,
                fam_oop,
                ind_ded_met,
                fam_ded_met,
                ind_oop_met,
                fam_oop_met,
            ),
        )
        upsert_count += 1

    return upsert_count


def detect_accumulator_anomalies(conn) -> int:
    snapshot_rows = conn.execute(
        """
        SELECT
            s.snapshot_id,
            s.member_id,
            s.family_id,
            s.client_id,
            s.plan_id,
            s.benefit_year,
            s.individual_deductible_accum,
            s.family_deductible_accum,
            s.individual_oop_accum,
            s.family_oop_accum,
            p.individual_deductible,
            p.family_deductible,
            p.individual_oop_max,
            p.family_oop_max
        FROM accumulator_snapshots s
        JOIN benefit_plans p
          ON s.plan_id = p.plan_id
        """
    ).fetchall()

    created = 0

    for row in snapshot_rows:
        anomalies = []

        if float(row["individual_deductible_accum"]) < 0 or float(row["family_deductible_accum"]) < 0 or float(row["individual_oop_accum"]) < 0 or float(row["family_oop_accum"]) < 0:
            anomalies.append(
                ("ACCUMULATOR", "NEGATIVE_ACCUMULATOR", "MEDIUM", f"Negative accumulator detected for member {row['member_id']}")
            )

        if float(row["individual_oop_accum"]) > float(row["individual_oop_max"]):
            anomalies.append(
                (
                    "ACCUMULATOR",
                    "IND_OOP_EXCEEDS_MAX",
                    "CRITICAL",
                    f"Individual OOP accumulator {row['individual_oop_accum']} exceeds max {row['individual_oop_max']} for member {row['member_id']}",
                )
            )

        if float(row["family_oop_accum"]) > float(row["family_oop_max"]):
            anomalies.append(
                (
                    "ACCUMULATOR",
                    "FAM_OOP_EXCEEDS_MAX",
                    "CRITICAL",
                    f"Family OOP accumulator {row['family_oop_accum']} exceeds max {row['family_oop_max']} for family {row['family_id']}",
                )
            )

        # Family rollup check: compare subscriber's family_oop_accum against sum of all members' individual_oop_accum
        subscriber_row = conn.execute(
            "SELECT subscriber_id FROM members WHERE family_id = ? AND member_id = subscriber_id LIMIT 1",
            (row["family_id"],),
        ).fetchone()

        if subscriber_row and row["member_id"] == subscriber_row["subscriber_id"]:
            fam_sum_row = conn.execute(
                """
                SELECT
                    SUM(individual_oop_accum) AS sum_individual_oop,
                    SUM(individual_deductible_accum) AS sum_individual_ded
                FROM accumulator_snapshots
                WHERE family_id = ?
                  AND plan_id = ?
                  AND benefit_year = ?
                """,
                (row["family_id"], row["plan_id"], row["benefit_year"]),
            ).fetchone()

            if fam_sum_row:
                sum_ind_oop = float(fam_sum_row["sum_individual_oop"] or 0)
                if abs(sum_ind_oop - float(row["family_oop_accum"])) > 0.01:
                    anomalies.append(
                        (
                            "ACCUMULATOR",
                            "FAMILY_ROLLUP_MISMATCH",
                            "MEDIUM",
                            f"Family rollup mismatch for family {row['family_id']} plan_id {row['plan_id']}: subscriber family_oop_accum={row['family_oop_accum']} vs summed individual_oop={sum_ind_oop}",
                        )
                    )

        for issue_type, subtype, severity, description in anomalies:
            entity_key = f"{row['member_id']}|{row['plan_id']}|{row['benefit_year']}|{subtype}"
            exists = conn.execute(
                """
                SELECT 1
                FROM data_quality_issues
                WHERE issue_type = ?
                  AND issue_subtype = ?
                  AND entity_key = ?
                LIMIT 1
                """,
                (issue_type, subtype, entity_key),
            ).fetchone()

            if exists:
                continue

            create_issue(
                conn=conn,
                issue_type=issue_type,
                issue_subtype=subtype,
                severity=severity,
                status="OPEN",
                client_id=row["client_id"],
                member_id=row["member_id"],
                entity_name="accumulator_snapshots",
                entity_key=entity_key,
                issue_description=description,
            )
            created += 1

    return created