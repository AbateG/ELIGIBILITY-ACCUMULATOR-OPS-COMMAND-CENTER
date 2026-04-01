import hashlib
from pathlib import Path


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def compute_file_hash(file_path: Path) -> str:
    hasher = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def count_file_rows(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8") as f:
        return max(sum(1 for _ in f) - 1, 0)