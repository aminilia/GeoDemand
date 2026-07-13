from __future__ import annotations

import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    runtime_root = project_root / ".test-runtime"
    runtime_dir = runtime_root / f"pytest-{uuid.uuid4().hex}"
    cache_dir = project_root / ".cache" / "pytest"

    runtime_dir.mkdir(parents=True, exist_ok=False)
    cache_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["TMP"] = str(runtime_dir)
    env["TEMP"] = str(runtime_dir)
    env["TMPDIR"] = str(runtime_dir)

    command = [
        sys.executable,
        "-m",
        "pytest",
        "-o",
        f"cache_dir={cache_dir}",
        *sys.argv[1:],
    ]
    try:
        completed = subprocess.run(command, cwd=project_root, env=env, check=False)
        return completed.returncode
    finally:
        shutil.rmtree(runtime_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
