"""Financial projection utilities for 529Estimator."""
from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple, Union

try:  # Optional NumPy support for accelerated simulations
    import numpy as _np  # type: ignore
except Exception:  # pragma: no cover - NumPy is optional at runtime
    _np = None

# MonthlySchedule amounts represent per-calendar-month contributions regardless
# of the simulation precision (periods per year).
MonthlySchedule = Tuple[float, Optional[float], float]
MonthlySeq = Sequence[MonthlySchedule]

HAS_NUMPY = _np is not None


def _age_to_month_start(age: float, start_age: float, m: int, max_month: int) -> int:
    """Return the first month index (>=0) whose ending age is >= the given age."""

    idx = math.ceil((age - start_age) * m - 1e-9)
    if idx < 0:
        idx = 0
    if idx > max_month:
        idx = max_month
    return idx


def _age_to_month_end(age: float, start_age: float, m: int, max_month: int) -> int:
    """Return the last month index (<=max_month) whose ending age is <= the given age."""

    idx = math.floor((age - start_age) * m + 1e-9)
    if idx < 0:
        idx = 0
    if idx > max_month:
        idx = max_month
    return idx


def _normalize_monthlies(
    monthly: Optional[Union[MonthlySchedule, MonthlySeq]]
) -> List[MonthlySchedule]:
    if not monthly:
        return []
    if isinstance(monthly, tuple) and len(monthly) == 3:
        return [monthly]
    normalized: List[MonthlySchedule] = []
    for item in monthly:  # type: ignore[iteration-over-optional]
        if not item:
            continue
        if len(item) != 3:
            raise ValueError(
                f"Monthly schedule {item!r} must be (start, end, amount)"
            )
        start, end, amt = item
        normalized.append(
            (float(start), float(end) if end is not None else None, float(amt))
        )
    return normalized


def _pre_start_value_and_adjust(
    lump_events: List[Tuple[float, float]],
    monthly: Optional[Union[MonthlySchedule, MonthlySeq]],
    r: float,
    start_age: float,
    horizon_age: float,
    m: int = 12,
):
    """Roll contributions before start_age forward and trim inputs to the analysis window."""

    horizon = max(horizon_age, start_age)
    total_periods = int(round((horizon - start_age) * m))

    i = r / m
    pre_value = 0.0
    post_lumps: List[Tuple[float, float]] = []
    monthly_by_period: Dict[int, float] = {}

    for age, amt in lump_events:
        if age < start_age:
            months = int(math.ceil((start_age - age) * m - 1e-9))
            if months > 0:
                pre_value += amt * ((1 + i) ** months)
            else:
                pre_value += amt
        elif age <= horizon:
            post_lumps.append((age, amt))

    for sched in _normalize_monthlies(monthly):
        m_start, m_end, m_amt = sched
        eff_end = m_end if m_end is not None else horizon
        eff_end = min(eff_end, horizon)
        if eff_end <= m_start:
            continue

        start_month_idx = int(math.ceil((m_start - start_age) * 12 - 1e-9))
        end_month_idx = int(math.floor((eff_end - start_age) * 12 + 1e-9))
        if end_month_idx < start_month_idx:
            continue

        for month_idx in range(start_month_idx, end_month_idx + 1):
            contrib_age = start_age + month_idx / 12.0
            if contrib_age < m_start - 1e-9 or contrib_age > eff_end + 1e-9:
                continue
            if contrib_age < start_age - 1e-9:
                months_forward = int(
                    math.ceil((start_age - contrib_age) * m - 1e-9)
                )
                if months_forward > 0:
                    pre_value += m_amt * ((1 + i) ** months_forward)
                else:
                    pre_value += m_amt
                continue
            if contrib_age > horizon + 1e-9:
                continue
            period_idx = _age_to_month_end(contrib_age, start_age, m, total_periods)
            if 0 <= period_idx <= total_periods:
                monthly_by_period[period_idx] = (
                    monthly_by_period.get(period_idx, 0.0) + m_amt
                )

    return pre_value, post_lumps, monthly_by_period


def timeline_hybrid_py(
    lump_events: List[Tuple[float, float]],
    monthly: Optional[Union[MonthlySchedule, MonthlySeq]],
    r: float,
    start_age: int,
    end_age: int,
    m: int = 12,
) -> List[Tuple[int, float]]:
    """Pure-Python monthly simulation with yearly snapshots."""

    pre_val, lumps_after, monthly_by_period = _pre_start_value_and_adjust(
        lump_events, monthly, r, start_age, end_age, m
    )
    value = pre_val
    out = []
    i = r / m
    end_month = int(round((end_age - start_age) * m))

    lump_by_month: Dict[int, float] = {}
    for age, amt in lumps_after:
        mi = _age_to_month_end(age, start_age, m, end_month)
        lump_by_month[mi] = lump_by_month.get(mi, 0.0) + amt

    monthly_vec = [0.0] * (end_month + 1)
    for mi, amt in monthly_by_period.items():
        if 0 <= mi <= end_month:
            monthly_vec[mi] += amt

    for mi in range(0, end_month + 1):
        if mi in lump_by_month:
            value += lump_by_month[mi]
        value *= (1 + i)
        if monthly_vec[mi]:
            value += monthly_vec[mi]
        if mi % m == 0:
            age = start_age + mi // m
            out.append((age, value))
    return out


def timeline_hybrid_np(
    lump_events: List[Tuple[float, float]],
    monthly: Optional[Union[MonthlySchedule, MonthlySeq]],
    r: float,
    start_age: int,
    end_age: int,
    m: int = 12,
) -> List[Tuple[int, float]]:
    """Vectorized monthly simulation with yearly snapshots (requires NumPy)."""

    if not HAS_NUMPY:
        return timeline_hybrid_py(lump_events, monthly, r, start_age, end_age, m)

    assert _np is not None  # for type checkers
    i = r / m
    months = int(round((end_age - start_age) * m))
    growth = (1 + i)

    pre_val, lumps_after, monthly_by_period = _pre_start_value_and_adjust(
        lump_events, monthly, r, start_age, end_age, m
    )
    values = _np.empty(months + 1, dtype=_np.float64)
    values[0] = pre_val

    lump_vec = _np.zeros(months + 1, dtype=_np.float64)
    for age, amt in lumps_after:
        mi = _age_to_month_end(age, start_age, m, months)
        if 0 <= mi <= months:
            lump_vec[mi] += amt

    add_vec = _np.zeros(months + 1, dtype=_np.float64)
    for mi, amt in monthly_by_period.items():
        if 0 <= mi <= months:
            add_vec[mi] += amt

    months_idx = _np.arange(months + 1, dtype=_np.float64)
    powers = growth ** months_idx
    adj = growth * lump_vec + add_vec
    conv = _np.convolve(adj, powers, mode="full")[: months + 1]
    values = (growth ** (months_idx + 1)) * pre_val + conv

    years = [start_age + k // m for k in range(0, months + 1, m)]
    vals = [float(values[k]) for k in range(0, months + 1, m)]
    return list(zip(years, vals))


def timeline_hybrid(
    lump_events,
    monthly,
    r,
    start_age,
    end_age,
    m=12,
    use_numpy=True,
):
    if use_numpy and HAS_NUMPY:
        return timeline_hybrid_np(lump_events, monthly, r, start_age, end_age, m)
    return timeline_hybrid_py(lump_events, monthly, r, start_age, end_age, m)


def value_at_age_hybrid_exact(
    lump_events,
    monthly,
    r,
    target_age_float,
    start_age=1,
    m=12,
):
    """Value at fractional age (year + month/12). Uses python path for simplicity/clarity."""

    horizon = max(target_age_float, start_age)
    pre_val, lumps_after, monthly_by_period = _pre_start_value_and_adjust(
        lump_events, monthly, r, start_age, horizon, m
    )
    i = r / m
    if target_age_float < start_age:
        months = int(round((start_age - target_age_float) * m))
        return pre_val / ((1 + i) ** months)

    max_month = int(round((horizon - start_age) * m))
    lump_by_month: Dict[int, float] = {}
    for age, amt in lumps_after:
        mi = _age_to_month_end(age, start_age, m, max_month)
        lump_by_month[mi] = lump_by_month.get(mi, 0.0) + amt

    end_month = int(round((target_age_float - start_age) * m))
    monthly_vec = [0.0] * (end_month + 1)
    for mi, amt in monthly_by_period.items():
        if 0 <= mi <= end_month:
            monthly_vec[mi] += amt
    value = pre_val
    for mi in range(0, end_month + 1):
        if mi in lump_by_month:
            value += lump_by_month[mi]
        value *= (1 + i)
        if monthly_vec[mi]:
            value += monthly_vec[mi]
    return value


def reality_diff(expected_value: float, observed_value: float) -> Dict[str, float]:
    delta = observed_value - expected_value
    pct = (delta / expected_value * 100.0) if expected_value != 0 else float("inf")
    return {
        "expected": expected_value,
        "observed": observed_value,
        "difference": delta,
        "difference_pct": pct,
    }


__all__ = [
    "HAS_NUMPY",
    "MonthlySchedule",
    "MonthlySeq",
    "timeline_hybrid",
    "timeline_hybrid_np",
    "timeline_hybrid_py",
    "value_at_age_hybrid_exact",
    "reality_diff",
]
