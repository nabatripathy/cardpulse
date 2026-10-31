"""Wait until every URL answers HTTP 2xx (or the timeout passes).

Used by the docker profile so the CardPulse runner starts only after the Iceberg
REST catalog and Marquez are ready, instead of racing them.

    python tools/wait_for.py http://iceberg-rest:8181/v1/config http://marquez:5000/api/v1/namespaces
"""

from __future__ import annotations

import argparse
import sys
import time
import urllib.error
import urllib.request


def ready(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:  # noqa: S310 - URLs come from the compose file
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError):
        return False


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("urls", nargs="+")
    p.add_argument("--timeout", type=float, default=180, help="seconds to wait in total")
    args = p.parse_args(argv)
    deadline = time.monotonic() + args.timeout
    pending = list(args.urls)
    while pending and time.monotonic() < deadline:
        pending = [u for u in pending if not ready(u)]
        if pending:
            time.sleep(2)
    for u in args.urls:
        print(f"{'ready  ' if u not in pending else 'TIMEOUT'} {u}")
    return 1 if pending else 0


if __name__ == "__main__":
    sys.exit(main())
