"""CI guard: fail if any committed text file contains a Luhn-valid card number.

Phase 3 security control: project environments hold synthetic data only, and a CI
check blocks real card-number patterns. The only numbers allowed are the public
test card numbers listed in ALLOWED.

    python tools/scan_repo_for_pans.py [paths...]
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "frameworks"))

from cardpulse_frameworks.common.pan import _CANDIDATE, luhn_valid  # noqa: E402

ALLOWED = {"4111111111111111", "5555555555554444", "378282246310005", "6011111111111117"}
TEXT_SUFFIXES = {".py", ".yaml", ".yml", ".md", ".toml", ".json", ".sql", ".txt", ".csv", ".dat", ".cfg", ".ini"}


def tracked_files() -> list[Path]:
    try:
        cmd = ["git", "ls-files"]  # fixed command, no user input
        out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True).stdout  # noqa: S603
        return [ROOT / f for f in out.splitlines()]
    except (OSError, subprocess.CalledProcessError):
        return [p for p in ROOT.rglob("*") if p.is_file() and ".git" not in p.parts and ".lakehouse" not in p.parts]


def main(argv: list[str]) -> int:
    files = [Path(a) for a in argv] or tracked_files()
    findings = []
    for f in files:
        if f.suffix not in TEXT_SUFFIXES or not f.is_file():
            continue
        for n, line in enumerate(f.read_text(errors="ignore").splitlines(), 1):
            for m in _CANDIDATE.finditer(line):
                digits = re.sub(r"[ -]", "", m.group())
                if luhn_valid(digits) and digits not in ALLOWED:
                    findings.append(f"{f.relative_to(ROOT) if f.is_absolute() else f}:{n}: {digits[:6]}******")
    for x in findings:
        print(x)
    print(f"{len(findings)} possible card numbers found" if findings else "No card numbers found")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
