"""529Estimator package entry points."""
from __future__ import annotations

import sys

# Avoid leaving ``__pycache__`` folders behind when the estimator runs.
sys.dont_write_bytecode = True

from .cli import build_parser, run_cli
from .gui.app import run as run_gui

__all__ = ["build_parser", "run_cli", "run_gui", "main", "main_cli"]


def main(argv=None) -> None:
    """Console entry point supporting both CLI and GUI modes."""

    parser = build_parser()
    default_args = parser.parse_args([])
    args = parser.parse_args(argv)

    if args.gui:
        run_gui()
        return

    if args.cli:
        run_cli(args)
        return

    cli_fields = [
        "rates",
        "start_age",
        "end_age",
        "lumps",
        "monthly",
        "hybrid_monthly",
        "combined",
        "colors",
        "periods",
        "engine",
        "parallel",
        "workers",
    ]
    if any(getattr(args, field) != getattr(default_args, field) for field in cli_fields):
        parser.error("CLI options require --cli; add --cli to run command-line mode.")

    run_gui()


def main_cli(argv=None) -> None:
    """Dedicated console entry point for CLI usage."""

    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.cli:
        args.cli = True
    run_cli(args)
