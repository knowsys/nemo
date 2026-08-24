#!/usr/bin/env bash
# Fix the Nix devcontainer's libstdc++ problem for the repo virtualenv.
#
# The bundled libzmq of the pyzmq wheel needs libstdc++.so.6 at runtime.
# On this Nix-based devcontainer that library is only on LD_LIBRARY_PATH
# inside `nix develop`, so `jupyter lab`, notebook kernels, etc. fail with
# "libstdc++.so.6: cannot open shared object file" outside it.
#
# This script finds a libstdc++ that actually works with the venv and
# embeds its directory into the pyzmq shared libraries via patchelf, so
# every command works without LD_LIBRARY_PATH.
#
# Re-run after a container rebuild or after reinstalling/upgrading pyzmq.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"

# Already fine?
if "${VENV_PYTHON}" -c 'import zmq' >/dev/null 2>&1; then
    echo "pyzmq already works; nothing to do."
    exit 0
fi

command -v patchelf >/dev/null || { echo "error: patchelf not found"; exit 1; }

# Find a libstdc++ directory that makes pyzmq importable.
LIBDIR=""
for dir in \
    /nix/store/*-gcc-*-lib/lib \
    /usr/lib/aarch64-linux-gnu \
    /usr/lib/x86_64-linux-gnu \
    /usr/lib \
    /usr/lib64
do
    [ -f "${dir}/libstdc++.so.6" ] || continue
    if {
        env LD_LIBRARY_PATH="${dir}" "${VENV_PYTHON}" -c 'import zmq' >/dev/null 2>&1
    } 2>/dev/null
    then
        LIBDIR="${dir}"
        break
    fi
done

if [ -z "${LIBDIR}" ]; then
    echo "error: no working libstdc++ found" >&2
    exit 1
fi
echo "using libstdc++ from: ${LIBDIR}"

# Embed the directory into the pyzmq shared libraries.
for so in "${REPO_ROOT}"/.venv/lib/python3.13/site-packages/pyzmq.libs/*.so* \
          "${REPO_ROOT}"/.venv/lib/python3.13/site-packages/zmq/backend/cython/_zmq*.so
do
    [ -f "${so}" ] || continue
    patchelf --add-rpath "${LIBDIR}" "${so}"
    echo "patched: ${so##*/}"
done

"${VENV_PYTHON}" -c 'import zmq; print("pyzmq OK:", zmq.__version__)'
