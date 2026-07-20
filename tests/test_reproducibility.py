from __future__ import annotations

import pyarrow as pa

from geodemand.reproducibility import canonical_table_content_hash


def test_canonical_content_hash_is_stable_across_row_and_column_order() -> None:
    first = pa.table({"episode_id": ["b", "a"], "value": [2.0, 1.0]})
    second = pa.table({"value": [1.0, 2.0], "episode_id": ["a", "b"]})
    assert canonical_table_content_hash(first) == canonical_table_content_hash(second)
