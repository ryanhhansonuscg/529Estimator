"""Reporting helpers such as CSV export."""
from __future__ import annotations

import csv
import numbers
from typing import Iterable, Sequence


def export_csv(path: str, header: Sequence[str], rows: Iterable[Sequence]):
    """Write header/row data to a CSV file."""

    def _format_cell(value: object) -> str:
        """Format cells to avoid decimal places in numeric output."""

        if isinstance(value, numbers.Real):
            # Format all real numbers with no decimal places (rounded).
            return format(value, ".0f")
        return str(value)

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([_format_cell(h) for h in header])
        for row in rows:
            writer.writerow([_format_cell(cell) for cell in row])


__all__ = ["export_csv"]
