from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_run_tests_prefers_repository_source_over_external_package(tmp_path: Path) -> None:
    root = Path(__file__).parents[1].resolve()
    fake_root = tmp_path / "external"
    fake_package = fake_root / "geodemand"
    fake_package.mkdir(parents=True)
    (fake_package / "__init__.py").write_text(
        "raise RuntimeError('stale external geodemand imported')\n", encoding="utf-8"
    )
    probe = tmp_path / "test_import_probe.py"
    expected = str((root / "src" / "geodemand").resolve())
    probe.write_text(
        "from pathlib import Path\n"
        "import geodemand\n\n"
        "def test_local_source():\n"
        f"    assert str(Path(geodemand.__file__).resolve().parent) == {expected!r}\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(fake_root)
    completed = subprocess.run(
        [sys.executable, str(root / "scripts" / "run_tests.py"), str(probe), "-q"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
