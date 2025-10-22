"""Computation helpers and concurrency utilities."""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import List, Optional, Tuple

from .finance import HAS_NUMPY, timeline_hybrid
from .parsing import combine_monthlies


def resolve_use_numpy(engine: str) -> bool:
    """Return True if the NumPy engine should be used for simulations."""

    normalized = engine.lower()
    if normalized not in {"auto", "numpy", "python"}:
        raise ValueError(f"Unknown engine '{engine}' (expected auto/numpy/python)")
    if normalized == "numpy":
        if not HAS_NUMPY:
            raise RuntimeError("NumPy requested but not installed.")
        return True
    if normalized == "python":
        return False
    return HAS_NUMPY


def resolve_parallel_mode(mode: str, task_count: int) -> str:
    """Return concrete parallel mode (process/thread/none) given the request."""

    normalized = mode.lower()
    if normalized not in {"auto", "process", "thread", "none"}:
        raise ValueError(f"Unknown parallel mode '{mode}'")
    if task_count <= 1:
        return "none"
    if normalized == "auto":
        return "process"
    return normalized


def normalize_workers(workers: Optional[int]) -> Optional[int]:
    if workers is None or workers <= 0:
        return None
    return workers


def execute_parallel(func, tasks: List[Tuple], mode: str, workers: Optional[int]):
    """Execute a callable for each argument tuple using the requested parallel mode."""

    if mode == "none" or not tasks:
        return [func(*args) for args in tasks]
    Executor = ProcessPoolExecutor if mode == "process" else ThreadPoolExecutor
    with Executor(max_workers=normalize_workers(workers)) as executor:
        futures = [executor.submit(func, *args) for args in tasks]
        return [f.result() for f in futures]


def resolve_strategy_monthlies(monthly_base, monthly_extra):
    """Return normalized schedules for monthly-only and hybrid strategies."""

    monthly_only = list(monthly_base) if monthly_base else []
    hybrid_only = combine_monthlies(None, monthly_extra)
    return monthly_only, hybrid_only


def compute_one(
    rate: float,
    lumps,
    monthly_base,
    monthly_extra,
    start_age,
    end_age,
    m,
    use_numpy: bool = True,
):
    """Pure function executed in worker processes for fast mode."""

    monthly_only, hybrid_monthlies = resolve_strategy_monthlies(
        monthly_base, monthly_extra
    )
    return {
        "rate": rate,
        "lump": timeline_hybrid(lumps, None, rate, start_age, end_age, m, use_numpy),
        "monthly": timeline_hybrid([], monthly_only, rate, start_age, end_age, m, use_numpy)
        if monthly_only
        else None,
        "hybrid": timeline_hybrid(
            lumps, hybrid_monthlies, rate, start_age, end_age, m, use_numpy
        ),
    }


def table_compute_one(
    rate: float,
    lumps,
    monthly_base,
    start_age,
    end_age,
    m,
    use_numpy,
    ages,
    monthly_extra=None,
):
    """Helper used when building the comparison table (picklable for process pool)."""

    monthly_only, hybrid_monthlies = resolve_strategy_monthlies(
        monthly_base, monthly_extra
    )
    lc = timeline_hybrid(lumps, None, rate, start_age, end_age, m, use_numpy)
    if monthly_only:
        mc = timeline_hybrid([], monthly_only, rate, start_age, end_age, m, use_numpy)
    else:
        mc = [(a, 0.0) for a in ages]
    hc = timeline_hybrid(
        lumps, hybrid_monthlies, rate, start_age, end_age, m, use_numpy
    )
    return rate, {a: v for a, v in lc}, {a: v for a, v in mc}, {a: v for a, v in hc}


__all__ = [
    "compute_one",
    "execute_parallel",
    "normalize_workers",
    "resolve_strategy_monthlies",
    "resolve_parallel_mode",
    "resolve_use_numpy",
    "table_compute_one",
]
