"""Create the lakehouse bucket on the S3-compatible store if it does not exist yet.

Reads the bucket, endpoint and credentials from an environment profile (the
catalog's ``warehouse`` and ``s3.*`` properties), and retries while the store
starts up.

    python tools/ensure_bucket.py --env docker
"""

from __future__ import annotations

import argparse
import sys
import time
from urllib.parse import urlparse


def main(argv: list[str] | None = None) -> int:
    from pyarrow import fs

    from cardpulse_frameworks.common.config import load_env

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--env", default="docker")
    p.add_argument("--timeout", type=float, default=120)
    args = p.parse_args(argv)
    props = load_env(args.env).catalog.properties
    warehouse = urlparse(props.get("warehouse", ""))
    if warehouse.scheme != "s3":
        print(f"warehouse is {props.get('warehouse')!r}, not s3://; nothing to do")
        return 0
    endpoint = urlparse(props["s3.endpoint"])
    s3 = fs.S3FileSystem(
        access_key=props.get("s3.access-key-id"),
        secret_key=props.get("s3.secret-access-key"),
        region=props.get("s3.region", "us-east-1"),
        endpoint_override=endpoint.netloc,
        scheme=endpoint.scheme or "http",
        allow_bucket_creation=True,
    )
    bucket = warehouse.netloc
    deadline, last = time.monotonic() + args.timeout, None
    while time.monotonic() < deadline:
        try:
            if s3.get_file_info(bucket).type == fs.FileType.NotFound:
                s3.create_dir(bucket)
                print(f"created bucket {bucket} at {endpoint.geturl()}")
            else:
                print(f"bucket {bucket} exists at {endpoint.geturl()}")
            return round_trip(s3, bucket)
        except OSError as e:  # store still starting
            last = e
            time.sleep(2)
    print(f"could not reach or create bucket {bucket}: {last}", file=sys.stderr)
    return 1


def round_trip(s3, bucket: str) -> int:
    """Write an object and read it back: an S3 store that alters uploads would corrupt Iceberg files."""
    payload = bytes(range(256)) * 4096  # 1 MiB, every byte value
    path = f"{bucket}/_cardpulse_health/round_trip.bin"
    with s3.open_output_stream(path) as out:
        out.write(payload)
    with s3.open_input_stream(path) as src:
        back = src.read()
    s3.delete_file(path)
    if back != payload:
        print(
            f"S3 round trip FAILED: wrote {len(payload)} bytes, read {len(back)} bytes back "
            f"(first bytes {back[:24]!r}); the store is altering uploads",
            file=sys.stderr,
        )
        return 1
    print(f"S3 round trip OK ({len(payload):,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
