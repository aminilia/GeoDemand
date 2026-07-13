from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, SupportsFloat, cast

import polars as pl

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "event_id",
        "event_date",
        "country",
        "region_id",
        "region_name",
        "latitude",
        "longitude",
        "flood_severity",
        "source",
    }
)


class GroundsourceError(ValueError):
    """Raised when Groundsource input cannot be processed."""


@dataclass(frozen=True)
class GroundsourceInspection:
    input_path: Path
    rows: int
    columns: list[str]
    required_columns: list[str]

    @property
    def missing_columns(self) -> list[str]:
        return sorted(REQUIRED_COLUMNS.difference(self.columns))

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_path": str(self.input_path),
            "rows": self.rows,
            "columns": self.columns,
            "required_columns": self.required_columns,
            "missing_columns": self.missing_columns,
        }


def inspect_groundsource(input_path: Path) -> GroundsourceInspection:
    path = _validate_input_path(input_path)
    frame = _scan_groundsource(path)
    schema = frame.collect_schema()
    columns = list(schema.names())
    _validate_required_columns(columns)
    rows = _collect_row_count(frame)
    LOGGER.info("groundsource_inspected", extra={"input_path": str(path), "rows": rows})
    return GroundsourceInspection(
        input_path=path,
        rows=rows,
        columns=columns,
        required_columns=sorted(REQUIRED_COLUMNS),
    )


def filter_groundsource(
    input_path: Path,
    output_path: Path,
    country: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> Path:
    path = _validate_input_path(input_path)
    normalized_country = _normalize_country(country)
    frame = _scan_groundsource(path)
    _validate_required_columns(frame.collect_schema().names())

    filtered = _with_event_date(frame).filter(pl.col("country") == normalized_country)
    if start_date is not None:
        filtered = filtered.filter(pl.col("event_date") >= start_date)
    if end_date is not None:
        filtered = filtered.filter(pl.col("event_date") <= end_date)

    output_path.mkdir(parents=True, exist_ok=True)
    rows = _collect_row_count(filtered)
    if rows == 0:
        raise GroundsourceError(
            "No Groundsource records matched the requested filters. "
            "Check --country, --start-date, and --end-date."
        )

    filtered.collect().write_parquet(
        output_path,
        mkdir=True,
        partition_by=["country", "event_date"],
    )
    LOGGER.info(
        "groundsource_filtered",
        extra={"input_path": str(path), "output_path": str(output_path), "rows": rows},
    )
    return output_path


def profile_groundsource(input_path: Path, output_path: Path) -> dict[str, Any]:
    path = _validate_input_path(input_path)
    frame = _scan_groundsource(path)
    schema = frame.collect_schema()
    _validate_required_columns(schema.names())

    normalized = _with_event_date(frame)
    profile = {
        "input_path": str(path),
        "rows": _collect_row_count(normalized),
        "columns": list(schema.names()),
        "schema": {name: str(dtype) for name, dtype in schema.items()},
        "date_range": _date_range(normalized),
        "country_counts": _country_counts(normalized),
        "missing_counts": _missing_counts(normalized),
        "numeric_summary": _numeric_summary(normalized),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(profile, indent=2, sort_keys=True), encoding="utf-8")
    LOGGER.info(
        "groundsource_profiled",
        extra={"input_path": str(path), "output_path": str(output_path)},
    )
    return profile


def _scan_groundsource(path: Path) -> pl.LazyFrame:
    try:
        return pl.scan_parquet(path)
    except Exception as exc:  # pragma: no cover - message wrapper around engine exceptions
        message = f"Unable to read Groundsource Parquet file at {path}: {exc}"
        raise GroundsourceError(message) from exc


def _validate_input_path(input_path: Path) -> Path:
    path = input_path.expanduser().resolve()
    if not path.exists():
        raise GroundsourceError(f"Groundsource input path does not exist: {path}")
    if path.is_dir():
        message = f"Groundsource input path must be a Parquet file, got directory: {path}"
        raise GroundsourceError(message)
    return path


def _validate_required_columns(columns: list[str]) -> None:
    missing = sorted(REQUIRED_COLUMNS.difference(columns))
    if missing:
        raise GroundsourceError(
            "Groundsource input is missing required columns: " + ", ".join(missing)
        )


def _normalize_country(country: str) -> str:
    normalized = country.strip().upper()
    if len(normalized) not in {2, 3}:
        raise GroundsourceError("Country must be a 2- or 3-character country code.")
    return normalized


def _with_event_date(frame: pl.LazyFrame) -> pl.LazyFrame:
    return frame.with_columns(pl.col("event_date").cast(pl.Date, strict=False))


def _collect_row_count(frame: pl.LazyFrame) -> int:
    result = frame.select(pl.len().alias("rows")).collect()
    return int(result.item(0, "rows"))


def _date_range(frame: pl.LazyFrame) -> dict[str, str | None]:
    result = frame.select(
        pl.col("event_date").min().alias("min_event_date"),
        pl.col("event_date").max().alias("max_event_date"),
    ).collect()
    return {
        "min_event_date": _optional_string(result.item(0, "min_event_date")),
        "max_event_date": _optional_string(result.item(0, "max_event_date")),
    }


def _country_counts(frame: pl.LazyFrame) -> dict[str, int]:
    result = (
        frame.group_by("country").agg(pl.len().alias("rows")).sort("country").collect().to_dicts()
    )
    return {str(row["country"]): int(row["rows"]) for row in result}


def _missing_counts(frame: pl.LazyFrame) -> dict[str, int]:
    result = frame.select(
        [pl.col(column).is_null().sum().alias(column) for column in sorted(REQUIRED_COLUMNS)]
    ).collect()
    row = result.to_dicts()[0]
    return {column: int(value) for column, value in row.items()}


def _numeric_summary(frame: pl.LazyFrame) -> dict[str, dict[str, float | None]]:
    columns = ["latitude", "longitude", "flood_severity"]
    result = frame.select(
        [
            expression
            for column in columns
            for expression in (
                pl.col(column).min().alias(f"{column}_min"),
                pl.col(column).max().alias(f"{column}_max"),
                pl.col(column).mean().alias(f"{column}_mean"),
            )
        ]
    ).collect()
    row = result.to_dicts()[0]
    return {
        column: {
            "min": _optional_float(row[f"{column}_min"]),
            "max": _optional_float(row[f"{column}_max"]),
            "mean": _optional_float(row[f"{column}_mean"]),
        }
        for column in columns
    }


def _optional_float(value: object) -> float | None:
    return None if value is None else float(cast(SupportsFloat, value))


def _optional_string(value: object) -> str | None:
    return None if value is None else str(value)
