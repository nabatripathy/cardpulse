"""Serve the generated dbt docs site; Ctrl-C stops it quietly.

    python tools/serve_docs.py .lakehouse/gold/dbt/docs [--port 8080]

A plain static server: the dbt docs site is index.html plus manifest.json and catalog.json,
so `dbt docs serve` (which prints a traceback on Ctrl-C) is not needed.
"""

from __future__ import annotations

import argparse
import functools
import http.server
import sys


class _Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        pass


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("directory")
    p.add_argument("--port", type=int, default=8080)
    args = p.parse_args(argv)
    handler = functools.partial(_Quiet, directory=args.directory)
    with http.server.ThreadingHTTPServer(("127.0.0.1", args.port), handler) as server:
        print(f"dbt docs: http://localhost:{args.port}  (Ctrl-C to stop)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nstopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
