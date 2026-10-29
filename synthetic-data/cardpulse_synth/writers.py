"""Write files the way the real senders do.

* Card processor: pipe-delimited header/detail/trailer (H/D/T) files.
* Preference center and campaign system: CSV with a header row.

Landing layout: <landing>/<source>/<entity>/business_date=YYYY-MM-DD/<file>
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from datetime import date
from pathlib import Path


def landing_dir(root: Path, source: str, entity: str, business_date: date) -> Path:
    d = root / source / entity / f"business_date={business_date.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def hdt_file_name(source: str, entity: str, business_date: date, seq: int) -> str:
    return f"{source}_{entity}_{business_date:%Y%m%d}_{seq:04d}.dat"


def write_hdt(
    root: Path,
    source: str,
    entity: str,
    file_type: str,
    business_date: date,
    seq: int,
    columns: Sequence[str],
    rows: Sequence[Sequence[str]],
    amount_field: str | None = None,
    trailer_count_adjust: int = 0,
) -> Path:
    """Write one H/D/T file. The trailer holds the detail count and amount total."""
    path = landing_dir(root, source, entity, business_date) / hdt_file_name(source, entity, business_date, seq)
    amount_idx = columns.index(amount_field) if amount_field else None
    total = 0
    with open(path, "w", newline="\n") as fh:
        # Deterministic creation time (seq x 15 minutes) so the same seed gives byte-identical files.
        minutes = seq * 15 % 1440
        created = f"{business_date.isoformat()}T{minutes // 60:02d}:{minutes % 60:02d}:00Z"
        fh.write(f"H|{file_type}|{business_date:%Y%m%d}|{seq:04d}|{created}\n")
        for row in rows:
            if amount_idx is not None:
                total += int(row[amount_idx])
            fh.write("D|" + "|".join(row) + "\n")
        fh.write(f"T|{len(rows) + trailer_count_adjust}|{total}\n")
    return path


def write_csv(
    root: Path,
    source: str,
    entity: str,
    business_date: date,
    seq: int,
    columns: Sequence[str],
    rows: Sequence[Sequence[str]],
) -> Path:
    name = f"{source}_{entity}_{business_date:%Y%m%d}_{seq:04d}.csv"
    path = landing_dir(root, source, entity, business_date) / name
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(columns)
        w.writerows(rows)
    return path
