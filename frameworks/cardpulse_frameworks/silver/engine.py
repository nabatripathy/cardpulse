"""DuckDB session for Silver transforms.

DuckDB runs the SQL over Arrow tables that PyIceberg reads and writes, so no
DuckDB Iceberg extension (and no network access) is needed. The macros below keep
the column expressions in the specs short and consistent across entities.
"""

from __future__ import annotations

import duckdb

# Processor time-zone codes -> IANA zones (DST handled by ICU).
TZ_ZONES = {
    "ET": "America/New_York",
    "CT": "America/Chicago",
    "MT": "America/Denver",
    "PT": "America/Los_Angeles",
}

MACROS = {
    # '' or whitespace -> NULL; everything else trimmed
    "blank(x)": "nullif(trim(x), '')",
    # YYYYMMDD -> DATE (NULL if blank or invalid)
    "ymd(x)": "CAST(try_strptime(nullif(trim(x), ''), '%Y%m%d') AS DATE)",
    # integer cents -> dollars
    "cents(x)": "TRY_CAST(nullif(trim(x), '') AS DECIMAL(18,0)) / 100",
    # Y/N -> BOOLEAN
    "yn(x)": "CASE upper(trim(x)) WHEN 'Y' THEN TRUE WHEN 'N' THEN FALSE END",
    # local date + HHMMSS + processor zone code -> UTC instant
    "local_ts(d, t, tz)": (
        "timezone(CASE upper(trim(tz)) "
        + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in TZ_ZONES.items())
        + " ELSE 'America/New_York' END, "
        "try_strptime(nullif(trim(d), '') || coalesce(nullif(trim(t), ''), '000000'), '%Y%m%d%H%M%S'))"
    ),
}


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(
        config={"autoinstall_known_extensions": False, "autoload_known_extensions": False, "threads": 4}
    )
    con.execute("SET TimeZone = 'UTC'")  # TIMESTAMPTZ values leave DuckDB as UTC (Iceberg requirement)
    for sig, body in MACROS.items():
        con.execute(f"CREATE OR REPLACE MACRO {sig} AS {body}")
    return con
