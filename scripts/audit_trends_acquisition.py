from __future__ import annotations

from pathlib import Path

from geodemand.trends_acquisition import (
    compare_acquisition_reproducibility,
    inventory_acquisition_stores,
)


def main() -> None:
    data_root = Path("C:/Work/Data/GeoDemand/trends")
    output_root = Path("reports/acquisition_audit")
    inventory_acquisition_stores(data_root, output_root / "inventory")
    compare_acquisition_reproducibility(
        data_root / "raw",
        data_root / "raws",
        output_root / "reproducibility",
    )


if __name__ == "__main__":
    main()
