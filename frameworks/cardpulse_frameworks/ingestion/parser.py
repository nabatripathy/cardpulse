"""Parse one landed file and apply the Bronze guards.

File-level checks reject the whole file (nothing is written):
  HEADER_INVALID, TRAILER_MISSING, TRAILER_MISMATCH, SCHEMA_MISMATCH, QUARANTINE_THRESHOLD

Record-level checks quarantine one record (the rest of the file loads):
  RECORD_TYPE, FIELD_COUNT, REQUIRED_MISSING, PAN_DETECTED

Bronze keeps values exactly as received (strings). Typing, deduplication and
reference checks happen in Silver. Quarantined records are always redacted, so a
clear card number is never written anywhere (DQ-02).
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from cardpulse_frameworks.common.pan import contains_pan, is_pan, redact

from .spec import SourceSpec


@dataclass
class QuarantinedRecord:
    row_num: int
    reason: str
    detail: str
    raw_record: str  # always redacted


@dataclass
class ParseResult:
    path: Path
    business_date: date
    seq: int
    checksum: str
    rows_read: int = 0
    rows: list[list[str]] = field(default_factory=list)
    row_nums: list[int] = field(default_factory=list)
    row_hashes: list[str] = field(default_factory=list)
    quarantined: list[QuarantinedRecord] = field(default_factory=list)
    duplicate_keys: int = 0
    reject_reason: str | None = None
    reject_detail: str | None = None

    @property
    def rejected(self) -> bool:
        return self.reject_reason is not None

    def reject(self, reason: str, detail: str) -> ParseResult:
        self.reject_reason, self.reject_detail = reason, detail
        return self


def file_identity(spec: SourceSpec, path: Path) -> tuple[date, int]:
    m = re.fullmatch(spec.file_name_regex, path.name)
    if not m:
        raise ValueError(f"{path.name} does not match {spec.file_name_regex}")
    return datetime.strptime(m["business_date"], "%Y%m%d").date(), int(m["seq"])


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_file(spec: SourceSpec, path: Path) -> ParseResult:
    bdate, seq = file_identity(spec, path)
    result = ParseResult(path=path, business_date=bdate, seq=seq, checksum=sha256_file(path))
    if spec.format == "hdt":
        _parse_hdt(spec, path, result)
    else:
        _parse_csv(spec, path, result)
    if result.rejected:
        return result
    _count_duplicates(spec, result)
    nq = len(result.quarantined)
    if result.rows_read and nq > max(spec.max_quarantine_pct / 100 * result.rows_read, spec.quarantine_floor):
        pct = 100 * nq / result.rows_read
        result.reject(
            "QUARANTINE_THRESHOLD",
            f"{nq} records ({pct:.2f}%) quarantined; limit is {spec.max_quarantine_pct}% or "
            f"{spec.quarantine_floor} records, whichever is higher",
        )
    return result


def _check_record(spec: SourceSpec, values: list[str], row_num: int, raw: str, result: ParseResult) -> None:
    for col, v in zip(spec.columns, values, strict=True):
        if (col.pan_guard == "exact" and is_pan(v)) or (col.pan_guard == "scan" and contains_pan(v)):
            result.quarantined.append(QuarantinedRecord(row_num, "PAN_DETECTED", col.name, redact(raw)))
            return
    missing = [c.name for c, v in zip(spec.columns, values, strict=True) if c.required and not v.strip()]
    if missing:
        result.quarantined.append(QuarantinedRecord(row_num, "REQUIRED_MISSING", ",".join(missing), redact(raw)))
        return
    result.rows.append(values)
    result.row_nums.append(row_num)
    result.row_hashes.append(hashlib.sha256(raw.encode()).hexdigest())


def _parse_hdt(spec: SourceSpec, path: Path, result: ParseResult) -> None:
    d = spec.delimiter
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    if not lines or not lines[0].startswith(f"H{d}"):
        result.reject("HEADER_INVALID", "first record is not a header")
        return
    header = lines[0].split(d)
    if len(header) < 4 or header[1] != spec.file_type:
        result.reject("HEADER_INVALID", f"file type {header[1:2]} != {spec.file_type}")
        return
    if header[2] != f"{result.business_date:%Y%m%d}" or int(header[3]) != result.seq:
        result.reject("HEADER_INVALID", "header date or sequence does not match the file name")
        return
    if len(lines) < 2 or not lines[-1].startswith(f"T{d}"):
        result.reject("TRAILER_MISSING", "last record is not a trailer")
        return
    trailer = lines[-1].split(d)
    body = lines[1:-1]
    details = [(i + 2, ln) for i, ln in enumerate(body) if ln.startswith(f"D{d}")]
    result.rows_read = len(body)

    # Trailer reconciliation runs on the raw records, before any quarantine (DQ-07).
    if spec.trailer:
        try:
            exp_count, exp_total = int(trailer[1]), int(trailer[2])
        except (IndexError, ValueError):
            result.reject("TRAILER_MISMATCH", f"unreadable trailer {lines[-1]!r}")
            return
        if spec.trailer.record_count and exp_count != len(details):
            result.reject("TRAILER_MISMATCH", f"trailer count {exp_count} != {len(details)} detail records")
            return
        if spec.trailer.amount_total_field:
            idx = spec.column_names.index(spec.trailer.amount_total_field) + 1
            try:
                total = sum(int(ln.split(d)[idx]) for _, ln in details)
            except (IndexError, ValueError) as e:
                result.reject("TRAILER_MISMATCH", f"amount not readable for reconciliation: {e}")
                return
            if total != exp_total:
                result.reject("TRAILER_MISMATCH", f"trailer total {exp_total} != {total}")
                return

    ncols = len(spec.columns)
    for row_num, line in enumerate(body, start=2):
        if not line.startswith(f"D{d}"):
            result.quarantined.append(QuarantinedRecord(row_num, "RECORD_TYPE", line[:1], redact(line)))
            continue
        values = line.split(d)[1:]
        if len(values) != ncols:
            result.quarantined.append(
                QuarantinedRecord(row_num, "FIELD_COUNT", f"{len(values)} fields, expected {ncols}", redact(line))
            )
            continue
        _check_record(spec, values, row_num, line, result)


def _parse_csv(spec: SourceSpec, path: Path, result: ParseResult) -> None:
    with open(path, encoding="utf-8", newline="") as fh:
        text = fh.read()
    reader = csv.reader(io.StringIO(text), delimiter=spec.delimiter)
    try:
        header = next(reader)
    except StopIteration:
        result.reject("HEADER_INVALID", "empty file")
        return
    if header != spec.column_names:
        result.reject("SCHEMA_MISMATCH", f"columns {header} != spec {spec.column_names}")
        return
    ncols = len(header)
    for row_num, values in enumerate(reader, start=2):
        result.rows_read += 1
        raw = spec.delimiter.join(values)
        if len(values) != ncols:
            result.quarantined.append(
                QuarantinedRecord(row_num, "FIELD_COUNT", f"{len(values)} fields, expected {ncols}", redact(raw))
            )
            continue
        _check_record(spec, values, row_num, raw, result)


def _count_duplicates(spec: SourceSpec, result: ParseResult) -> None:
    if not spec.key_columns:
        return
    idx = [spec.column_names.index(k) for k in spec.key_columns]
    counts = Counter(tuple(r[i] for i in idx) for r in result.rows)
    result.duplicate_keys = sum(c - 1 for c in counts.values() if c > 1)
