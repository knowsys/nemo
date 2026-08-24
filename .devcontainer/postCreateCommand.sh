#!/usr/bin/env bash
# Post-create setup for the nemo devcontainer.
#
# Runs inside the Nix dev shell (see devcontainer.json postCreateCommand),
# which provides python3, maturin, wasm-pack, nodejs, etc.
#
# Steps:
#   1. Create a Python virtualenv (kept out of the Nix store so it's writable
#      and persists across rebuilds via the mounted volume).
#   2. Install the Jupyter tooling (notebook, lab, ipykernel, ...) into the venv.
#   3. Build the nmo_python bindings with maturin and install them into the venv.
set -euo pipefail

VENV_DIR="${WORKSPACE_FOLDER:-/workspaces/nemo}/.venv"
REQ_FILE="${WORKSPACE_FOLDER:-/workspaces/nemo}/.devcontainer/requirements-dev.txt"

echo "==> Creating Python virtualenv at ${VENV_DIR}"
python3 -m venv "${VENV_DIR}"

VENV_PYTHON="${VENV_DIR}/bin/python"
VENV_PIP="${VENV_DIR}/bin/pip"

echo "==> Installing Jupyter tooling"
"${VENV_PIP}" install --upgrade pip
"${VENV_PIP}" install -r "${REQ_FILE}"

echo "==> Activating virtualenv"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

echo "==> Building nmo_python bindings with maturin"
# maturin develop installs into the *active* virtualenv, so we activate the
# venv first (rather than the Nix Python) to ensure the bindings land there.
maturin develop --manifest-path "${WORKSPACE_FOLDER:-/workspaces/nemo}/nemo-python/Cargo.toml"

echo "==> Verifying nmo_python import"
python -c "import nmo_python; print('nmo_python OK:', nmo_python.__file__)"

echo "==> Installing nemo-jupyter package (kernel + nbconvert exporter)"
# Editable install so kernel.py/nbconvert.py changes are picked up live;
# registers the 'nemo' nbconvert script exporter used by "Export as
# Executable Script".
"${VENV_PIP}" install -e "${WORKSPACE_FOLDER:-/workspaces/nemo}/nemo-jupyter"

echo "==> Patching pyzmq so it finds libstdc++ outside nix develop"
# The kernel and Jupyter run outside the nix dev shell, where LD_LIBRARY_PATH
# has no libstdc++; embed a working path into the pyzmq shared libraries.
if [ -x "${VENV_DIR}/../nemo-jupyter/fix-venv-libstdcxx.sh" ]; then
    bash "${VENV_DIR}/../nemo-jupyter/fix-venv-libstdcxx.sh"
fi

echo "==> Devcontainer setup complete."
