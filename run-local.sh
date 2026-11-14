#!/usr/bin/env bash
# Bootstrap the project environment, then let Python supervise local services.
# First-run tools and .env setup are documented in docs/local-prerequisites.md.
set -euo pipefail
cd -- "$(dirname -- "$0")"

if [[ ! -x .venv/bin/python ]]; then
    if [[ -x ../.venv/bin/python ]]; then
        ln -s ../.venv .venv
    elif command -v python3.14 >/dev/null 2>&1; then
        python3.14 -m venv .venv
    else
        python3 -m venv .venv
    fi
fi

# PyArrow 26+ has wheels for Python 3.14; fail on unsupported versions.
.venv/bin/python - <<'PY'
import sys
if not (3, 10) <= sys.version_info[:2] <= (3, 14):
    raise SystemExit("Use a Python 3.10–3.14 virtual environment (3.14 recommended).")
PY

# Install only when a pinned dependency is missing or has a different version.
# Later starts reuse the environment without contacting the package index.
if ! .venv/bin/python - <<'PY'
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
for requirement in Path("requirements.txt").read_text().splitlines():
    if not requirement.strip() or requirement.startswith("#"):
        continue
    package, expected_version = requirement.split("==")
    try:
        if version(package) != expected_version:
            raise SystemExit(1)
    except PackageNotFoundError:
        raise SystemExit(1)
PY
then
    .venv/bin/python -m pip install -r requirements.txt
fi

# The supervisor replaces the existing WikiPulse stack before starting a fresh one.
exec .venv/bin/python -m src.run_local "$@"
