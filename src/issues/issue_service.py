def create_issue(
    conn,
    issue_type: str,
    issue_subtype: str | None,
    severity: str,
    status: str,
    issue_description: str,
    client_id: int | None = None,
    vendor_id: int | None = None,
    file_id: int | None = None,
    run_id: int | None = None,
    member_id: str | None = None,
    claim_record_id: int | None = None,
    entity_name: str | None = None,
    entity_key: str | None = None,
    source_row_number: int | None = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO data_quality_issues (
            issue_type, issue_subtype, severity, status,
            client_id, vendor_id, file_id, run_id,
            member_id, claim_record_id, entity_name, entity_key,
            source_row_number, issue_description
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            issue_type,
            issue_subtype,
            severity,
            status,
            client_id,
            vendor_id,
            file_id,
            run_id,
            member_id,
            claim_record_id,
            entity_name,
            entity_key,
            source_row_number,
            issue_description,
        ),
    )
    return cursor.lastrowid