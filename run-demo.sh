#!/usr/bin/env bash
set -euo pipefail

# WikiPulse 1-Command Presentation & Backup Demo Launcher
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python3"
if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="python3"
fi

export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
exec "${PYTHON_BIN}" "${SCRIPT_DIR}/src/demo/backup_demo.py" "$@"

