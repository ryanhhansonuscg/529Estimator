"""Input parsing helpers for 529Estimator."""
from __future__ import annotations

from typing import List, Optional

from .finance import MonthlySchedule, MonthlySeq


def parse_rates(text: str) -> List[float]:
    """Parse a comma-separated string of percentages into decimal rates."""

    return [float(s.strip()) / 100.0 for s in text.split(",") if s.strip()]


def parse_lumps(text: str) -> List[tuple[float, float]]:
    """Parse semicolon-separated "age:amount" pairs."""

    if not text.strip():
        return []
    parts = [p.strip() for p in text.split(";")]
    events = []
    for part in parts:
        age, amt = part.split(":")
        events.append((float(age), float(amt)))
    return events


def parse_monthly(text: str) -> List[MonthlySchedule]:
    """Parse semicolon-separated "start-end amount" segments."""

    if not text.strip():
        return []
    schedules: List[MonthlySchedule] = []
    parts = [p.strip() for p in text.split(";") if p.strip()]
    for part in parts:
        tokens = part.split()
        if len(tokens) != 2:
            raise ValueError(
                f"Monthly segment '{part}' must look like 'start-end amount'"
            )
        range_part, amt_part = tokens
        if "-" not in range_part:
            raise ValueError(f"Range '{range_part}' must contain '-'")
        start_s, end_s = range_part.split("-", 1)
        start_age = float(start_s)
        end_age = float(end_s) if end_s.strip() else None
        amt = float(amt_part)
        schedules.append((start_age, end_age, amt))
    return schedules


def combine_monthlies(
    base: Optional[MonthlySeq], extra: Optional[MonthlySeq]
) -> List[MonthlySchedule]:
    """Merge two monthly schedules while normalizing numeric types."""

    combined: List[MonthlySchedule] = []
    for seq in (base, extra):
        if not seq:
            continue
        for start, end, amt in seq:
            combined.append(
                (float(start), float(end) if end is not None else None, float(amt))
            )
    return combined


__all__ = [
    "combine_monthlies",
    "parse_lumps",
    "parse_monthly",
    "parse_rates",
]
