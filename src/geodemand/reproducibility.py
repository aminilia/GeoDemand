from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def canonical_table_content_hash(source: Path | pa.Table) -> str:
    """Hash logical table content independently of row and column order."""
    table = pq.read_table(source) if isinstance(source, Path) else source
    columns = sorted(table.column_names)
    rows = [
        {column: _canonical_value(row.get(column)) for column in columns}
        for row in table.to_pylist()
    ]
    rows.sort(key=_canonical_json)
    payload = {"columns": columns, "rows": rows}
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _canonical_value(value: Any) -> Any:  # noqa: PLR0911
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, float) and not math.isfinite(value):
        return {"nonfinite_float": str(value)}
    if isinstance(value, dict):
        return {str(key): _canonical_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return value
