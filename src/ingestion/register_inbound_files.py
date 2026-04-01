from datetime import datetime
from pathlib import Path

from config.file_patterns import CLAIMS_FILE_REGEX, ELIGIBILITY_FILE_REGEX, infer_file_type
from config.settings import INBOUND_DIR
from src.common.db import db_session, fetch_one
from src.common.file_utils import compute_file_hash, count_file_rows
from src.issues.issue_service import create_issue


def parse_filename_metadata(file_name: str) -> dict:
    name_without_ext = file_name.replace(".csv", "")
    parts = name_without_ext.split("_")

    if len(parts) < 4:
        return {
            "file_type": infer_file_type(file_name),
            "client_code": None,
            "vendor_code": None,
            "expected_date": None,
        }

    expected_date = None
    if len(parts) > 3 and len(parts[3]) == 8 and parts[3].isdigit():
        expected_date = f"{parts[3][:4]}-{parts[3][4:6]}-{parts[3][6:8]}"

    return {
        "file_type": infer_file_type(file_name),
        "client_code": parts[1] if len(parts) > 1 else None,
        "vendor_code": parts[2] if len(parts) > 2 else None,
        "expected_date": expected_date,
    }


def resolve_client_id(conn, client_code: str | None):
    if not client_code:
        return None
    row = conn.execute(
        "SELECT client_id FROM clients WHERE client_code = ?",
        (client_code,),
    ).fetchone()
    return row["client_id"] if row else None


def resolve_vendor_id(conn, vendor_code: str | None):
    if not vendor_code:
        return None
    row = conn.execute(
        "SELECT vendor_id FROM vendors WHERE vendor_code = ?",
        (vendor_code,),
    ).fetchone()
    return row["vendor_id"] if row else None


def filename_matches_pattern(file_name: str, file_type: str | None) -> bool:
    if file_type == "ELIGIBILITY":
        return bool(ELIGIBILITY_FILE_REGEX.match(file_name))
    if file_type == "CLAIMS":
        return bool(CLAIMS_FILE_REGEX.match(file_name))
    return False


def register_inbound_files() -> None:
    INBOUND_DIR.mkdir(parents=True, exist_ok=True)

    file_paths = sorted(INBOUND_DIR.glob("*.csv"))
    if not file_paths:
        print("No inbound files found.")
        return

    with db_session() as conn:
        for file_path in file_paths:
            file_name = file_path.name
            metadata = parse_filename_metadata(file_name)
            file_type = metadata["file_type"]
            client_id = resolve_client_id(conn, metadata["client_code"])
            vendor_id = resolve_vendor_id(conn, metadata["vendor_code"])
            file_hash = compute_file_hash(file_path)
            row_count = count_file_rows(file_path)
            received_ts = datetime.now().isoformat(timespec="seconds")

            existing_by_name = conn.execute(
                "SELECT file_id FROM inbound_files WHERE file_name = ?",
                (file_name,),
            ).fetchone()

            if existing_by_name:
                continue

            existing_by_hash = conn.execute(
                "SELECT file_id FROM inbound_files WHERE file_hash = ?",
                (file_hash,),
            ).fetchone()

            duplicate_flag = 1 if existing_by_hash else 0

            cursor = conn.execute(
                """
                INSERT INTO inbound_files (
                    file_name, file_type, client_id, vendor_id, expected_date,
                    received_ts, file_hash, row_count, processing_status,
                    duplicate_flag, landing_path
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_name,
                    file_type or "UNKNOWN",
                    client_id,
                    vendor_id,
                    metadata["expected_date"],
                    received_ts,
                    file_hash,
                    row_count,
                    "RECEIVED",
                    duplicate_flag,
                    str(file_path),
                ),
            )
            file_id = cursor.lastrowid

            conn.execute(
                """
                INSERT INTO audit_log (event_type, entity_name, entity_key, file_id, actor, event_details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "FILE_REGISTERED",
                    "inbound_files",
                    str(file_id),
                    file_id,
                    "system",
                    f"Registered inbound file {file_name}",
                ),
            )

            if not filename_matches_pattern(file_name, file_type):
                create_issue(
                    conn=conn,
                    issue_type="FILE",
                    issue_subtype="BAD_FILE_NAME_PATTERN",
                    severity="MEDIUM",
                    status="OPEN",
                    file_id=file_id,
                    client_id=client_id,
                    vendor_id=vendor_id,
                    issue_description=f"Filename does not match expected pattern: {file_name}",
                    entity_name="inbound_files",
                    entity_key=str(file_id),
                )

            if duplicate_flag == 1:
                create_issue(
                    conn=conn,
                    issue_type="FILE",
                    issue_subtype="DUPLICATE_FILE",
                    severity="HIGH",
                    status="OPEN",
                    file_id=file_id,
                    client_id=client_id,
                    vendor_id=vendor_id,
                    issue_description=f"Duplicate inbound file detected based on hash: {file_name}",
                    entity_name="inbound_files",
                    entity_key=str(file_id),
                )

    print(f"Registered {len(file_paths)} inbound file(s).")


if __name__ == "__main__":
    register_inbound_files()