from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
from typer.testing import CliRunner

from geodemand.cli import app
from geodemand.trends_acquisition import (
    compare_acquisition_reproducibility,
    export_pytrends,
    inventory_acquisition_stores,
)

ROOT = Path(__file__).parents[1]
DATA_ROOT = Path("C:/Work/Data/GeoDemand/trends")


class _FakeFrame:
    empty = False

    def __init__(self) -> None:
        self._rows = [
            {"date": "2024-01-01", "weather": 1, "flood": 2, "isPartial": False},
            {"date": "2024-01-02", "weather": 3, "flood": 4, "isPartial": False},
        ]

    def reset_index(self) -> _FakeFrame:
        return self

    def to_dict(self, orient: str) -> list[dict[str, object]]:
        assert orient == "records"
        return self._rows


class _FakeClient:
    def build_payload(self, *args: object, **kwargs: object) -> None:
        self.args = args
        self.kwargs = kwargs

    def interest_over_time(self) -> _FakeFrame:
        return _FakeFrame()


def test_inventory_acquisition_stores_reports_both_backends(tmp_path: Path) -> None:
    outputs = inventory_acquisition_stores(DATA_ROOT, tmp_path)
    manual = json.loads(outputs["manual_store"].read_text(encoding="utf-8"))
    pytrends = json.loads(outputs["pytrends_store"].read_text(encoding="utf-8"))
    assert manual["file_count"] > 0
    assert pytrends["file_count"] > 0
    assert {row["acquisition_backend"] for row in manual["files"]} == {"manual_csv"}
    assert {row["acquisition_backend"] for row in pytrends["files"]} == {"pytrends"}
    assert all("sha256" in row for row in manual["files"])
    assert all("size_bytes" in row for row in pytrends["files"])


def test_export_pytrends_writes_manifest_with_deterministic_filenames(tmp_path: Path) -> None:
    plan = tmp_path / "plan.csv"
    plan.write_text(
        "request_id,concept_ids,request_start_date,request_end_date,geography\n"
        "request-1,weather;flood,2024-01-01,2024-02-01,US-CA\n",
        encoding="utf-8",
    )
    result = export_pytrends(plan, tmp_path / "out", pytrends_client=_FakeClient())
    manifest = json.loads(result["manifest"].read_text(encoding="utf-8"))
    assert manifest["backend"] == "pytrends"
    assert manifest["request_count"] == 1
    assert manifest["requests"][0]["source_file"] == "request-1.csv"
    assert manifest["requests"][0]["acquisition_backend"] == "pytrends"


def test_acquisition_reproducibility_compares_shared_request_ids(tmp_path: Path) -> None:
    manual = tmp_path / "manual"
    pytrends = tmp_path / "pytrends"
    manual.mkdir()
    pytrends.mkdir()
    _write_csv(
        manual / "request-1.csv",
        "date,weather,flood\n2024-01-01,1,0\n2024-01-02,2,1\n",
    )
    _write_csv(
        pytrends / "request-1.csv",
        "date,weather,flood,isPartial\n2024-01-01,1,0,False\n2024-01-02,2,2,False\n",
    )
    outputs = compare_acquisition_reproducibility(manual, pytrends, tmp_path / "out")
    rows = pq.read_table(outputs["reproducibility_parquet"]).to_pylist()
    assert len(rows) == 2
    assert rows[0]["request_id"] == "request-1"
    assert rows[0]["concept_id"] == "flood"
    assert "pearson_r" in rows[0]


def test_export_pytrends_cli_help_includes_command() -> None:
    result = CliRunner().invoke(app, ["trends", "--help"])
    assert result.exit_code == 0
    assert "export-pytrends" in result.output


def _write_csv(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
