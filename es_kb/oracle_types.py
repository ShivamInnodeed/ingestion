from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import DateTime, String, TypeDecorator
from sqlalchemy.dialects import oracle

# Matches values like: 16-JUN-2026 11:39:21.343593 AM +00:00
ORACLE_TS_TZ_FORMAT = "DD-MON-YYYY HH12:MI:SS.FF6 AM TZH:TZM"
ORACLE_TS_TZ_REGEX = re.compile(
    r"^(\d{2})-([A-Z]{3})-(\d{4}) "
    r"(\d{2}):(\d{2}):(\d{2})\.(\d{6}) "
    r"(AM|PM) "
    r"([+-]\d{2}):(\d{2})$"
)
_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_oracle_timestamp_tz(value: datetime) -> str:
    """Format a datetime as Oracle TIMESTAMP WITH TIME ZONE text."""
    dt = ensure_utc(value)
    month = dt.strftime("%b").upper()
    hour12 = dt.strftime("%I")
    ampm = dt.strftime("%p")
    micro = f"{dt.microsecond:06d}"
    offset = dt.utcoffset() or timedelta(0)
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    tz_hours, tz_minutes = divmod(total_minutes, 60)
    return (
        f"{dt.day:02d}-{month}-{dt.year} "
        f"{hour12}:{dt.strftime('%M:%S')}.{micro} {ampm} "
        f"{sign}{tz_hours:02d}:{tz_minutes:02d}"
    )


def parse_oracle_timestamp_tz(value: str) -> datetime:
    """Parse Oracle TIMESTAMP WITH TIME ZONE text into an aware datetime."""
    match = ORACLE_TS_TZ_REGEX.match(value.strip().upper())
    if not match:
        raise ValueError(f"Invalid Oracle timestamp format: {value!r}")

    day, month_name, year, hour12, minute, second, micro, ampm, tz_h, tz_m = match.groups()
    month = _MONTHS[month_name]
    hour = int(hour12) % 12
    if ampm == "PM":
        hour += 12

    tz_sign = 1 if tz_h.startswith("+") else -1
    tz_hours = tz_sign * int(tz_h[1:])
    tz_minutes = tz_sign * int(tz_m)
    tzinfo = timezone(timedelta(hours=tz_hours, minutes=tz_minutes))

    return datetime(
        int(year),
        month,
        int(day),
        hour,
        int(minute),
        int(second),
        int(micro),
        tzinfo=tzinfo,
    )


class TzDateTime(TypeDecorator[datetime]):
    """Timezone-aware datetime compatible with Oracle TIMESTAMP WITH TIME ZONE."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect: Any):
        if dialect.name == "oracle":
            return dialect.type_descriptor(oracle.TIMESTAMP(timezone=True))
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(self, value: datetime | None, dialect: Any) -> datetime | str | None:
        if value is None:
            return None
        if not isinstance(value, datetime):
            raise TypeError(f"Expected datetime, got {type(value)!r}")

        aware = ensure_utc(value)
        if dialect.name == "oracle":
            # Bind the exact Oracle text format expected by the target schema/tools.
            return format_oracle_timestamp_tz(aware)
        return aware

    def process_result_value(self, value: datetime | str | None, dialect: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return ensure_utc(value)
        if isinstance(value, str):
            return parse_oracle_timestamp_tz(value)
        raise TypeError(f"Unexpected timestamp value type: {type(value)!r}")
