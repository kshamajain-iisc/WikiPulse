#!/usr/bin/env bash
set -euo pipefail

# WikiPulse 1-Command Presentation & Backup Demo Launcher
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -x "${SCRIPT_DIR}/.venv/bin/python3" ]]; then
    PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python3"
elif [[ -x "${PROJECT_ROOT}/.venv/bin/python3" ]]; then
    PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python3"
else
    PYTHON_BIN="python3"
fi

export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
exec "${PYTHON_BIN}" "${SCRIPT_DIR}/src/demo/backup_demo.py" "$@"

