# /// script
# requires-python = ">=3.10"
# dependencies = ["pyiceberg[sql-sqlite,pyarrow]>=0.9"]
# ///
"""Writes a small Iceberg v2 table with pyiceberg (the reference Python
implementation) for the interop test in crates/plimsoll-connectors/tests/iceberg.rs.

    uv run scripts/iceberg_fixture.py <warehouse-dir>   # local SQL catalog
    uv run scripts/iceberg_fixture.py http://host:8181  # via an Iceberg REST catalog

Prints PLIMSOLL_ICEBERG_FIXTURE=<metadata location>.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog

target = sys.argv[1]
if target.startswith("http"):
    catalog = load_catalog("fixture", type="rest", uri=target)
else:
    warehouse = Path(target).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "fixture",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
catalog.create_namespace_if_not_exists("bronze")
if catalog.table_exists("bronze.trips"):
    catalog.drop_table("bronze.trips")

schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
    ]
)


def rows(ids):
    return pa.table(
        {
            "id": ids,
            "name": [f"n{i}" for i in ids],
            "ts": [datetime(2024, 1, i, tzinfo=timezone.utc) for i in ids],
        },
        schema=schema,
    )


table = catalog.create_table("bronze.trips", schema=schema)
table.append(rows(list(range(1, 6))))
table.append(rows(list(range(6, 11))))
table.delete("id <= 3")  # copy-on-write: rewrites the first file
print(f"PLIMSOLL_ICEBERG_FIXTURE={table.metadata_location}")
