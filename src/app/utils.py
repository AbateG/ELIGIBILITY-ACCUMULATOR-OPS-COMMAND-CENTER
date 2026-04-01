import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from datetime import datetime
import pandas as pd


def to_dataframe(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def safe_parse_ts(value):
    if value is None or value == "":
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def add_age_hours_column(df: pd.DataFrame, ts_col: str, new_col: str = "age_hours") -> pd.DataFrame:
    if df.empty or ts_col not in df.columns:
        return df

    df = df.copy()
    now = datetime.utcnow()

    def _calc(v):
        dt = safe_parse_ts(v)
        if not dt:
            return None
        return round((now - dt).total_seconds() / 3600, 1)

    df[new_col] = df[ts_col].apply(_calc)
    return df


def sort_priority_series(series: pd.Series) -> pd.Series:
    order = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}
    return series.map(lambda x: order.get(x, 99))


def bool_flag_to_label(value) -> str:
    if value in (1, True, "1"):
        return "Yes"
    return "No"