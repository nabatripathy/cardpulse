"""Fail if a Bronze/Silver/Gold run summary shows any work done (used to prove reruns are no-ops).

Reads the Markdown tables printed by `cardpulse-ingest run`, `cardpulse-silver run` and
`cardpulse-gold run` from a file and checks the "Loaded" and "Rejected" columns (Bronze)
and the "Units" column (Silver) are all zero, and that Gold restaged no Silver table
("Status" is UNCHANGED in the staging table).

    python tools/check_rerun_noop.py rerun.txt
"""

from __future__ import annotations

import sys


def main(path: str) -> int:
    header: list[str] | None = None
    checked, bad = 0, []
    for line in open(path, encoding="utf-8"):
        if not line.startswith("|"):
            header = None
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if set(line.strip()) <= {"|", "-", " "}:
            continue
        if header is None:
            header = cells
            continue
        for col in ("Loaded", "Rejected", "Units"):
            if col in header:
                checked += 1
                value = cells[header.index(col)].replace(",", "")
                if value != "0":
                    bad.append(f"{cells[0]}: {col} = {value}")
        if header[0] == "Silver table staged" and "Status" in header:
            checked += 1
            if cells[header.index("Status")] != "UNCHANGED":
                bad.append(f"{cells[0]}: restaged into Gold")
    if checked == 0:
        print("no run summary found", file=sys.stderr)
        return 1
    if bad:
        print("rerun did work:\n" + "\n".join(bad), file=sys.stderr)
        return 1
    print(f"rerun is a no-op ({checked} counters checked, all zero)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
