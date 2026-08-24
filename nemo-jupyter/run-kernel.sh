#!/usr/bin/env bash
# Launch the Nemo Jupyter kernel.
#
# pyzmq (used by ipykernel) is a manylinux wheel that needs libstdc++ at
# runtime. The Nix-based devcontainer does not expose libstdc++ on the
# default library path outside `nix develop`, so we probe for a working
# libstdc++ (system first, then the Nix store) and prepend it to
# LD_LIBRARY_PATH before starting the kernel.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"
LAUNCHER="${REPO_ROOT}/nemo-jupyter/run-kernel.py"

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
            env LD_LIBRARY_PATH="${dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" \
                "${VENV_PYTHON}" -c 'import zmq' >/dev/null 2>&1
        } 2>/dev/null
        then
            export LD_LIBRARY_PATH="${dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
            break
        fi
    done
fi

exec "${VENV_PYTHON}" "${LAUNCHER}" "$@"
