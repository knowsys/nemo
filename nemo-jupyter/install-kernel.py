#!/usr/bin/env python
"""Register the Nemo Jupyter kernel with Jupyter.

Writes a kernelspec into the user-level Jupyter data directory
(``~/.local/share/jupyter/kernels/nemo``). The kernelspec launches the
kernel through ``nemo-jupyter/run-kernel.sh``, which uses this
repository's virtualenv (``.venv``) and handles the Nix devcontainer's
libstdc++ quirk (pyzmq needs it on LD_LIBRARY_PATH).

Usage:
    .venv/bin/python nemo-jupyter/install-kernel.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

KERNEL_NAME = "nemo"
DISPLAY_NAME = "Nemo"

LIBCANDIDATES = [
    "/usr/lib/aarch64-linux-gnu",
    "/usr/lib/x86_64-linux-gnu",
    "/usr/lib",
    "/usr/lib64",
]


def libstdcxx_dir(venv_python: Path) -> str | None:
    """Return a directory whose libstdc++ lets pyzmq import, or None.

    pyzmq is a manylinux wheel and needs a recent libstdc++ at runtime;
    the Nix-based devcontainer only exposes one inside ``nix develop``.
    """
    candidates = list(LIBCANDIDATES)
    candidates.extend(sorted(str(p) for p in Path("/nix/store").glob("*-gcc-*-lib/lib")))
    for directory in candidates:
        if not Path(directory).joinpath("libstdc++.so.6").exists():
            continue
        env = {**os.environ, "LD_LIBRARY_PATH": directory}
        probe = subprocess.run(
            [str(venv_python), "-c", "import zmq"],
            capture_output=True,
            env=env,
        )
        if probe.returncode == 0:
            return directory
    return None


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    venv_python = repo_root / ".venv" / "bin" / "python"
    launcher = repo_root / "nemo-jupyter" / "run-kernel.py"
    wrapper = repo_root / "nemo-jupyter" / "run-kernel.sh"

    if not venv_python.exists():
        print(f"error: virtualenv python not found: {venv_python}", file=sys.stderr)
        print("Create it first, e.g. with the devcontainer postCreateCommand.sh.", file=sys.stderr)
        return 1
    for required in (launcher, wrapper):
        if not required.exists():
            print(f"error: {required} not found", file=sys.stderr)
            return 1

    # pyzmq needs libstdc++; pass a working LD_LIBRARY_PATH to the checks.
    env = dict(os.environ)
    libdir = libstdcxx_dir(venv_python)
    if libdir:
        env["LD_LIBRARY_PATH"] = libdir
    else:
        print("warning: no working libstdc++ found; the kernel may fail to start", file=sys.stderr)

    check = subprocess.run(
        [str(venv_python), "-c", "import nmo_python, ipykernel, jupyter_core"],
        capture_output=True,
        text=True,
        env=env,
    )
    if check.returncode != 0:
        print("error: kernel dependencies missing in the virtualenv:", file=sys.stderr)
        print(check.stderr.strip(), file=sys.stderr)
        return 1

    target = Path(os.environ.get("JUPYTER_DATA_DIR", "")) if os.environ.get("JUPYTER_DATA_DIR") else None
    if target is None:
        from jupyter_core.paths import jupyter_data_dir

        target = Path(jupyter_data_dir())
    target = target / "kernels" / KERNEL_NAME
    target.mkdir(parents=True, exist_ok=True)

    spec = {
        "argv": ["/bin/bash", str(wrapper), "-f", "{connection_file}"],
        "display_name": DISPLAY_NAME,
        "language": "nemo",
        "interrupt_mode": "message",
        "metadata": {"debugger": False},
    }
    kernel_json = target / "kernel.json"
    kernel_json.write_text(json.dumps(spec, indent=2) + "\n")

    print(f"installed kernel '{KERNEL_NAME}' -> {kernel_json}")
    print(f"  argv: {' '.join(spec['argv'][:2])} -f {{connection_file}}")
    if libdir:
        print(f"  libstdc++: {libdir}")

    list_result = subprocess.run(
        [str(venv_python), "-m", "jupyter", "kernelspec", "list"],
        capture_output=True,
        text=True,
        env=env,
    )
    if list_result.returncode == 0:
        print(list_result.stdout, end="")
    else:
        print("(could not run `jupyter kernelspec list` to verify)", file=sys.stderr)

    print()
    print("The kernel should now appear as 'Nemo' in VS Code / Jupyter.")
    print("If it does not show up, reload the VS Code window or restart the Jupyter server.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
