"""Command-line interface for 529Estimator."""
from __future__ import annotations

import argparse
import sys
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt

from .computation import (
    compute_one,
    execute_parallel,
    normalize_workers,
    resolve_parallel_mode,
    resolve_use_numpy,
)
from .finance import value_at_age_hybrid_exact
from .parsing import combine_monthlies, parse_lumps, parse_monthly, parse_rates


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--cli", action="store_true", help="Run in CLI mode")
    mode.add_argument("--gui", action="store_true", help="Run GUI")
    parser.add_argument("--rates", default="4,6,8", help="Comma-separated rates in % (e.g., '4,6,8')")
    parser.add_argument("--start-age", type=int, default=0)
    parser.add_argument("--end-age", type=int, default=25)
    parser.add_argument(
        "--lumps",
        default="0.5:5000",
        help="Semicolon-separated age:amount;... (pre-1 allowed, e.g., 0.5:1000)",
    )
    parser.add_argument(
        "--monthly",
        default="0-18 125",
        help="Semicolon-separated 'start-end amount' segments (pre-1 allowed, e.g., 0-18 125)",
    )
    parser.add_argument(
        "--hybrid-monthly",
        default="5-18 125",
        help="Semicolon-separated monthly segments added only to the Hybrid strategy",
    )
    parser.add_argument("--combined", action="store_true", help="Combined overlay chart (CLI)")
    parser.add_argument("--colors", default="", help="Comma-separated colors (hex or names) per rate")
    parser.add_argument(
        "--periods", type=int, default=12, help="Compounding periods per year (12=monthly, 4=quarterly, 1=yearly)"
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "numpy", "python"],
        default="auto",
        help="Computation engine: auto prefers NumPy when available",
    )
    parser.add_argument(
        "--parallel",
        choices=["auto", "process", "thread", "none"],
        default="auto",
        help="Parallel execution mode for per-rate calculations",
    )
    parser.add_argument("--workers", type=int, default=0, help="Worker count for parallel execution (0 = auto)")
    return parser


def _linestyle_for(label: str) -> str:
    if "Lump Sum" in label:
        return "-"
    if "Monthly" in label:
        return "--"
    return "-."


def _plot_series(
    series: Dict[str, List[Tuple[int, float]]],
    title: str,
    combined: bool,
    rates: Sequence[float],
    color_map: Dict[float, str],
):
    if not combined:
        return
    plt.figure(figsize=(10, 6))
    for label, curve in series.items():
        ages = [a for a, _ in curve]
        vals = [v for _, v in curve]
        rate_tag: Optional[float] = None
        for r in rates:
            if f"{int(r * 100)}%" in label:
                rate_tag = r
                break
        plt.plot(
            ages,
            vals,
            label=label,
            color=color_map.get(rate_tag),
            linestyle=_linestyle_for(label),
        )
    plt.title(title)
    plt.xlabel("Age")
    plt.ylabel("Estimated Value ($)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()


def run_cli(args: argparse.Namespace) -> None:
    rates = parse_rates(args.rates)
    lumps = parse_lumps(args.lumps)
    monthly_only = parse_monthly(args.monthly) if args.monthly else []
    monthly_extra = parse_monthly(args.hybrid_monthly) if getattr(args, "hybrid_monthly", "") else []
    hybrid_monthlies = combine_monthlies(monthly_only, monthly_extra)
    start_age = args.start_age
    end_age = args.end_age
    m = args.periods

    engine_mode = args.engine
    parallel_requested = args.parallel
    workers = normalize_workers(args.workers)
    try:
        use_numpy = resolve_use_numpy(engine_mode)
    except RuntimeError as exc:
        print(f"Warning: {exc} Falling back to pure Python engine.", file=sys.stderr)
        use_numpy = False

    parallel_mode = resolve_parallel_mode(parallel_requested, len(rates))
    if parallel_mode == "none" and parallel_requested not in {"auto", "none"} and len(rates) <= 1:
        print("Note: only one rate provided; parallel execution disabled.", file=sys.stderr)

    default_palette = ["#4E79A7", "#59A14F", "#E15759", "#9C755F", "#76B7B2"]
    color_map = {rates[i]: default_palette[i % len(default_palette)] for i in range(len(rates))}
    if args.colors:
        color_parts = [c.strip() for c in args.colors.split(",") if c.strip()]
        if len(color_parts) == len(rates):
            color_map = {rates[i]: color_parts[i] for i in range(len(rates))}

    tasks = [
        (r, lumps, monthly_only, monthly_extra, start_age, end_age, m, use_numpy)
        for r in rates
    ]
    results = execute_parallel(compute_one, tasks, parallel_mode, workers)

    series: Dict[str, List[Tuple[int, float]]] = {}
    for res in results:
        r = res["rate"]
        series[f"Lump Sum only @ {int(r * 100)}%"] = res["lump"]
        if res["monthly"] is not None:
            series[f"Monthly only @ {int(r * 100)}%"] = res["monthly"]
        series[f"Hybrid @ {int(r * 100)}%"] = res["hybrid"]

    _plot_series(
        series,
        f"529 Projections (Ages {start_age}–{end_age})",
        combined=args.combined,
        rates=rates,
        color_map=color_map,
    )
    plt.show(block=False)

    points = [30, 40, 50, 60]
    print("\nPOINT ESTIMATES (tables per rate)")
    for r in rates:
        print(f"\nRate {int(r * 100)}%")
        header = ["Option"] + [f"Age {a}" for a in points]
        print(" | ".join(header))
        for option in ["Lump Sum", "Monthly", "Hybrid"]:
            row = [option]
            for age in points:
                if option == "Lump Sum":
                    v = value_at_age_hybrid_exact(lumps, None, r, age, start_age, m)
                elif option == "Monthly":
                    v = (
                        value_at_age_hybrid_exact([], monthly_only, r, age, start_age, m)
                        if monthly_only
                        else 0.0
                    )
                else:
                    v = value_at_age_hybrid_exact(lumps, hybrid_monthlies, r, age, start_age, m)
                row.append(f"${v:,.2f}")
            print(" | ".join(row))
    plt.show()


__all__ = ["build_parser", "run_cli"]
