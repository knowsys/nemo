"""Shared helpers for turning notebook cells into Nemo source text.

Used by both the kernel (``kernel.py``) and the nbconvert exporter
(``nbconvert.py``) so the magic command names stay in one place.
"""

from __future__ import annotations

import os
import re

#: Kernel command (magic) names. Magics start with ``!`` because Nemo
#: reserves ``%`` for comments.
MAGIC_NAMES = (
    "help",
    "load",
    "predicates",
    "program",
    "pwd",
    "reset",
    "standalone",
    "trace",
    "version",
)

_LINE_MAGIC_RE = re.compile(r"^!(\w+)\s*(.*)$")


def clean_nemo_source(source: str, base_dir: str | None = None) -> str:
    """Remove kernel magics from a cell, leaving valid Nemo source.

    * ``!load <file>`` is replaced by the file's contents.
    * ``!standalone`` marker lines are dropped (the program below them
      stays, as part of the accumulated program).
    * Other magics (``!trace``, ``!program``, ``!reset``, ...) are
      dropped.
    * Everything else (facts, rules, directives, ``%`` comments) is kept
      unchanged. Lines starting with ``!`` that are not a known magic
      (e.g. existential variables in rule bodies) are kept.
    """
    lines: list[str] = []
    for line in source.splitlines():
        stripped = line.strip()
        match = _LINE_MAGIC_RE.match(stripped) if stripped.startswith("!") else None
        if match and match.group(1) in MAGIC_NAMES:
            name, arg = match.group(1), match.group(2).strip()
            if name == "load" and arg:
                path = arg.strip("\"'")
                if base_dir:
                    path = os.path.join(base_dir, path)
                try:
                    with open(path, encoding="utf-8") as handle:
                        lines.append(handle.read().rstrip())
                except OSError as exc:
                    lines.append(f"% !load failed for {path}: {exc}")
            # any other magic (and the !standalone marker) is dropped
            continue
        lines.append(line)
    return "\n".join(lines)
