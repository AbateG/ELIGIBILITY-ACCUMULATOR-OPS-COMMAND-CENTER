from pathlib import Path

import pandas as pd

from config.file_patterns import CLAIMS_FILE_REGEX, ELIGIBILITY_FILE_REGEX
from config.settings import REQUIRED_CLAIMS_COLUMNS, REQUIRED_ELIGIBILITY_COLUMNS
from src.issues.issue_service import create_issue


def validate_file_name_pattern(file_name: str, file_type: str) -> tuple[bool, str | None]:
    if file_type == "ELIGIBILITY" and not ELIGIBILITY_FILE_REGEX.match(file_name):
        return False, "Filename does not match ELIGIBILITY naming convention"
    if file_type == "CLAIMS" and not CLAIMS_FILE_REGEX.match(file_name):
        return False, "Filename does not match CLAIMS naming convention"
    return True, None


def validate_file_not_empty(file_path: Path) -> tuple[bool, str | None]:
    if file_path.stat().st_size == 0:
        return False, "File is empty"
    return True, None


def validate_csv_readable(file_path: Path) -> tuple[bool, str | None]:
    try:
        pd.read_csv(file_path, nrows=5)
        return True, None
    except Exception as exc:
        return False, f"File is not readable as CSV: {exc}"


def validate_required_columns(file_path: Path, file_type: str) -> tuple[bool, str | None]:
    df = pd.read_csv(file_path, nrows=1)
    actual_columns = set(df.columns)

    required = (
        set(REQUIRED_ELIGIBILITY_COLUMNS)
        if file_type == "ELIGIBILITY"
        else set(REQUIRED_CLAIMS_COLUMNS)
    )

    missing = sorted(required - actual_columns)
    if missing:
        return False, f"Missing required columns: {missing}"
    return True, None


def run_file_validations(conn, file_record: dict) -> list[int]:
    issue_ids = []
    file_id = file_record["file_id"]
    file_name = file_record["file_name"]
    file_type = file_record["file_type"]
    file_path = Path(file_record["landing_path"])
    client_id = file_record["client_id"]
    vendor_id = file_record["vendor_id"]

    checks = [
        ("BAD_FILE_NAME_PATTERN", validate_file_name_pattern(file_name, file_type)),
        ("EMPTY_FILE", validate_file_not_empty(file_path)),
        ("UNREADABLE_FILE", validate_csv_readable(file_path)),
    ]

    for subtype, (ok, message) in checks:
        if not ok:
            issue_id = create_issue(
                conn=conn,
                issue_type="FILE",
                issue_subtype=subtype,
                severity="HIGH" if subtype in {"EMPTY_FILE", "UNREADABLE_FILE"} else "MEDIUM",
                status="OPEN",
                file_id=file_id,
                client_id=client_id,
                vendor_id=vendor_id,
                issue_description=message,
                entity_name="inbound_files",
                entity_key=str(file_id),
            )
            issue_ids.append(issue_id)

    # only check columns if file can be read
    if file_type in {"ELIGIBILITY", "CLAIMS"}:
        ok, message = validate_required_columns(file_path, file_type)
        if not ok:
            issue_id = create_issue(
                conn=conn,
                issue_type="FILE",
                issue_subtype="MISSING_REQUIRED_COLUMNS",
                severity="HIGH",
                status="OPEN",
                file_id=file_id,
                client_id=client_id,
                vendor_id=vendor_id,
                issue_description=message,
                entity_name="inbound_files",
                entity_key=str(file_id),
            )
            issue_ids.append(issue_id)

    if file_record.get("duplicate_flag", 0) == 1:
        issue_id = create_issue(
            conn=conn,
            issue_type="FILE",
            issue_subtype="DUPLICATE_FILE",
            severity="HIGH",
            status="OPEN",
            file_id=file_id,
            client_id=client_id,
            vendor_id=vendor_id,
            issue_description=f"Duplicate inbound file detected for file_id={file_id}",
            entity_name="inbound_files",
            entity_key=str(file_id),
        )
        issue_ids.append(issue_id)

    new_status = "REJECTED" if issue_ids else "VALIDATED"
    conn.execute(
        """
        UPDATE inbound_files
        SET processing_status = ?, error_count = ?
        WHERE file_id = ?
        """,
        (new_status, len(issue_ids), file_id),
    )

    return issue_ids