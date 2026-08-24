#!/usr/bin/env bash
# Run the nemo-jupyter tests.
#
# pyzmq needs libstdc++ at runtime; on the Nix devcontainer it is only on
# LD_LIBRARY_PATH inside `nix develop`. This script probes for a working
# libstdc++ (system first, then the Nix store) before running the tests.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"

if ! "${VENV_PYTHON}" -c 'import zmq' >/dev/null 2>&1; then
    for dir in \
        /usr/lib/aarch64-linux-gnu \
        /usr/lib/x86_64-linux-gnu \
        /usr/lib \
        /usr/lib64 \
        /nix/store/*-gcc-*-lib/lib
    do
        [ -f "${dir}/libstdc++.so.6" ] || continue
        # probe in a block with this shell's stderr redirected, so that a
        # candidate which crashes (wrong libstdc++ version) stays silent
        if {
            env LD_LIBRARY_PATH="${dir}" "${VENV_PYTHON}" -c 'import zmq' >/dev/null 2>&1
        } 2>/dev/null
        then
            export LD_LIBRARY_PATH="${dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
            break
        fi
    done
fi

echo "==> unit tests"
"${VENV_PYTHON}" -m pytest "${REPO_ROOT}/nemo-jupyter/tests/" -q

echo
echo "==> end-to-end smoke test (starts a real kernel)"
"${VENV_PYTHON}" "${REPO_ROOT}/nemo-jupyter/tests/smoke_test.py"
