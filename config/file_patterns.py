import re

ELIGIBILITY_FILE_REGEX = re.compile(
    r"^ELIG_[A-Z0-9]+_[A-Z0-9]+_\d{8}(?:_[A-Z]+)?\.csv$"
)

CLAIMS_FILE_REGEX = re.compile(
    r"^CLAIMS_[A-Z0-9]+_[A-Z0-9]+_\d{8}(?:_[A-Z]+)?\.csv$"
)

def infer_file_type(file_name: str) -> str | None:
    if file_name.startswith("ELIG_"):
        return "ELIGIBILITY"
    if file_name.startswith("CLAIMS_"):
        return "CLAIMS"
    return None