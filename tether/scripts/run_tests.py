#!/usr/bin/env python3
"""Run unittest files separately without borrowing the operator's live runtime."""
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess  # nosec B404 - fixed interpreter and unittest argv
import sys
import tempfile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pattern", default="test_*.py", help="Test filename glob")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    files = sorted((root / "tests").glob(args.pattern))
    if not files or any(not file.is_file() or file.suffix != ".py" for file in files):
        parser.error("pattern must select Python test files")
    node = shutil.which("node")
    if not node:
        parser.error("Node.js 22 or 24 is required for CLI tests")
    failed = 0
    for file in files:
        with tempfile.TemporaryDirectory(prefix="tether-test-home-") as directory:
            home = Path(directory)
            binaries = home / "bin"
            binaries.mkdir()
            (binaries / "node").symlink_to(Path(node).resolve())
            (binaries / "python3").symlink_to(Path(sys.executable).resolve())
            # No ambient credentials, runtime socket, terminal placement or
            # model configuration reaches a test process. Leave XDG unset:
            # fixtures that replace HOME must get their own default paths.
            env = {
                "HOME": str(home),
                "PATH": f"{binaries}:/usr/bin:/bin",
                "LANG": "C.UTF-8",
                "HERMES_HOME": str(home / "hermes"),
                "CODEX_HOME": str(home / "codex"),
                "CLAUDE_HOME": str(home / "claude"),
                "CLAUDE_CONFIG_DIR": str(home / "claude"),
                "TETHER_HERDR": "off",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            try:
                result = subprocess.run(  # nosec B603 - fixed test entrypoint
                    [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", file.name, "-v"],
                    cwd=root, env=env, timeout=240, check=False,
                )
                code = result.returncode
            except subprocess.TimeoutExpired:
                code = 124
                print(f"TIMEOUT: {file.name} exceeded 240 seconds", file=sys.stderr)
            failed += code != 0
            print(f"{'FAIL' if code else 'PASS'}: {file.name}", flush=True)
    print(f"{len(files) - failed}/{len(files)} test files passed", flush=True)
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
