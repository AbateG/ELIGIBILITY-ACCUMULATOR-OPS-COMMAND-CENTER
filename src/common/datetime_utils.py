from datetime import datetime, timedelta, UTC


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(tzinfo=None, microsecond=0).isoformat(sep=" ")


def add_hours_iso(start_ts: str, hours: int) -> str:
    dt = datetime.fromisoformat(start_ts)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return (dt + timedelta(hours=hours)).replace(microsecond=0).isoformat(sep=" ")


def hours_elapsed(start_ts: str, end_ts: str) -> float:
    start_dt = datetime.fromisoformat(start_ts)
    end_dt = datetime.fromisoformat(end_ts)
    return (end_dt - start_dt).total_seconds() / 3600