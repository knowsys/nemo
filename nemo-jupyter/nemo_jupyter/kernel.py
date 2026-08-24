
"""Jupyter kernel for the Nemo rule engine.

Notebook cells accumulate into one Nemo program, like a normal notebook:
facts and rules from earlier cells are visible to later cells. On every
execution the accumulated program is reasoned over and the contents of all
``@export``-ed predicates are streamed back to the notebook.

The kernel builds on the experimental Python bindings in ``nemo-python``
(the ``nmo_python`` module), so its semantics follow those bindings. There
is no incremental engine API, so each cell re-runs the whole accumulated
program (``!reset`` clears it, ``!standalone`` runs one cell in isolation).
"""

from __future__ import annotations

import os
import re
import textwrap
from dataclasses import dataclass, field

from ipykernel.kernelbase import Kernel

import nmo_python

__all__ = [
    "NemoKernel",
    "ProgramResult",
    "format_fact",
    "format_value",
    "parse_export_predicates",
    "run_program",
    "strip_comments",
]

# ------------------------------------------------------------------ #
# Pure helpers (testable without a running kernel)                   #
# ------------------------------------------------------------------ #

def strip_comments(source: str) -> str:
    """Remove Nemo comments (``%`` line comments, ``/* ... */`` block comments).

    String literal contents are masked (replaced by ``""``) so that ``%``,
    ``/*`` or directive keywords inside strings neither start comments nor
    match directive patterns.
    """
    out: list[str] = []
    i, n = 0, len(source)
    in_block = False
    while i < n:
        if in_block:
            end = source.find("*/", i)
            if end == -1:
                break
            out.append(" ")
            i = end + 2
            in_block = False
        elif source.startswith("/*", i):
            in_block = True
            i += 2
        elif source[i] == '"':
            j = i + 1
            while j < n:
                if source[j] == "\\":
                    j += 2
                elif source[j] == '"':
                    j += 1
                    break
                else:
                    j += 1
            out.append('""')
            i = j
        elif source[i] == "%":
            j = i
            while j < n and source[j] not in "\r\n":
                j += 1
            out.append(" ")
            i = j
        else:
            out.append(source[i])
            i += 1
    return "".join(out)

_EXPORT_RE = re.compile(r"@export\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:/\d+)?")

def parse_export_predicates(source: str) -> list[str]:
    """Names of all ``@export``-ed predicates, in order of first appearance.

    The Python bindings currently cannot report output predicates
    reliably (see ``NemoProgram.output_predicates``), so the kernel
    extracts them from the (comment-stripped) program source. The
    predicate name directly follows the ``@export`` keyword and may be
    followed by an arity (``p/2`` or ``p(2)``), which is dropped.
    """
    names: list[str] = []
    for match in _EXPORT_RE.finditer(strip_comments(source)):
        name = match.group(1)
        if name not in names:
            names.append(name)
    return names

_IMPORT_RE = re.compile(r"@import\s+([A-Za-z_][A-Za-z0-9_]*)\s*:-")

def parse_import_predicates(source: str) -> list[str]:
    """Names of all ``@import``-ed predicates, in order of first appearance."""
    names: list[str] = []
    for match in _IMPORT_RE.finditer(strip_comments(source)):
        name = match.group(1)
        if name not in names:
            names.append(name)
    return names

def format_value(value: object) -> str:
    """Render a single result value in Nemo's canonical syntax."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        # Plain strings, IRIs and blank nodes already arrive in Nemo's
        # canonical form, e.g. '"hello"', '<http://ex.org/x>', '_:b0'.
        return value
    if isinstance(value, nmo_python.NemoLiteral):
        escaped = value.value().replace("\\", "\\\\").replace('"', '\\"')
        rendered = f'"{escaped}"'
        language = value.language()
        if language:
            return f"{rendered}@{language}"
        return f"{rendered}^^<{value.datatype()}>"
    return str(value)

def format_fact(predicate: str, values) -> str:
    """Render one result row as a Nemo fact, e.g. ``parent(<ada>, 42)``."""
    return f"{predicate}({', '.join(format_value(v) for v in values)})"

@dataclass
class ProgramResult:
    ok: bool
    text: str = ""
    error: str = ""
    predicates: list = field(default_factory=list)
    counts: dict = field(default_factory=dict)
    source: str = ""
    cell_count: int = 1
    timing: object = None
    program: object = None
    engine: object = None

def run_program(source: str) -> ProgramResult:
    """Parse, reason over and collect the exported predicates of a Nemo program."""
    try:
        program = nmo_python.load_string(source)
        engine = nmo_python.NemoEngine(program)
        engine.reason()
    except Exception as exc:  # nmo_python raises NemoError on parse/validation failures
        return ProgramResult(ok=False, error=str(exc))

    predicates = parse_export_predicates(source)
    counts: dict[str, int] = {}
    lines: list[str] = []
    for predicate in predicates:
        rows = list(engine.result(predicate))
        counts[predicate] = len(rows)
        lines.extend(format_fact(predicate, row) for row in rows)

    if not predicates:
        lines.append("(no @export directives found in this program)")
        lines.append('add e.g. "@export myPredicate :- csv {}." to see results')

    timing = None
    try:
        timing = engine.timing()
    except Exception:
        pass

    return ProgramResult(
        ok=True,
        text="\n".join(lines),
        source=source,
        predicates=predicates,
        counts=counts,
        timing=timing,
        program=program,
        engine=engine,
    )

# ------------------------------------------------------------------ #
# Kernel                                                             #
# ------------------------------------------------------------------ #

from .nemo_source import MAGIC_NAMES as _MAGICS

_HELP = """\
Nemo kernel — notebook cells accumulate into one Nemo program.

Write facts, rules and directives as in a .rls file. Earlier cells stay
active: a rule in cell 2 sees facts from cell 1. Every run re-reasons the
whole accumulated program and prints the contents of every @export-ed
predicate, e.g.

    parent(ada, bob) .
    ancestor(?x, ?y) :- parent(?x, ?y) .
    ancestor(?x, ?y) :- parent(?x, ?z), ancestor(?z, ?y) .
    @export ancestor :- csv {}.

Kernel commands (magics) start with '!' (Nemo uses '%' for comments):

    !help             this help
    !version          versions of kernel, bindings and rule engine
    !pwd              print the kernel's working directory
    !load <file>      print the contents of a Nemo (.rls) file
    !program          show the accumulated program (all cells so far)
    !reset            clear the accumulated program (start fresh)
    !standalone       run a cell in isolation (first line, program follows)
    !predicates       list exported and imported predicates of the last run
    !trace <fact>     show the derivation of a fact, e.g. !trace ancestor(ada, bob)

Notes
  * Only successful cells are accumulated; a failed cell changes nothing.
  * Re-running an identical cell is ignored (facts/rules are idempotent).
  * Editing an earlier cell does not retract its old content: restart the
    kernel and re-run all cells to rebuild a consistent program.
  * Redefining a @prefix/@base in a later cell is an error (as when
    concatenating .rls files).
  * @import directives resolve relative to the kernel's working directory
    (run !pwd to see it).
"""

def _nemo_version() -> str:
    try:
        from importlib import metadata

        return metadata.version("nmo-python")
    except Exception:
        return "unknown"

class NemoKernel(Kernel):
    implementation = "nemo_jupyter"
    implementation_version = "0.1.0"
    language = "nemo"
    language_version = _nemo_version()
    banner = "Nemo rule engine kernel (datalog reasoning for knowledge graphs)"
    language_info = {
        "name": "nemo",
        "mimetype": "text/x-nemo",
        "file_extension": ".rls",
        "pygments_lexer": "nemo",
        "codemirror_mode": "nemo",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._accumulated: list[str] = []
        self._last: ProgramResult | None = None

    # -- execute ---------------------------------------------------- #

    def do_execute(
        self, code, silent, store_history=True, user_expressions=None, allow_stdin=False
    ):
        text = (code or "").strip()
        if not text:
            return self._reply("ok")

        # "!standalone" on the first line runs this cell as an isolated
        # program (it neither sees nor changes the accumulated program).
        first_line, sep, rest = text.partition("\n")
        if first_line.strip() == "!standalone":
            program = rest.strip()
            if not program:
                if not silent:
                    self._send_error("MagicError", "usage: !standalone followed by a Nemo program on the next lines")
                return self._reply("error", "MagicError", "usage: !standalone followed by a Nemo program on the next lines")
            return self._execute_program(program, accumulate=False, silent=silent)

        magic = self._match_magic(text)
        if magic is not None:
            name, arg = magic
            try:
                output = self._run_magic(name, arg)
            except Exception as exc:
                if not silent:
                    self._send_error("MagicError", str(exc))
                return self._reply("error", "MagicError", str(exc))
            if not silent and output:
                self._stream(output)
            return self._reply("ok")

        return self._execute_program(text, accumulate=True, silent=silent)

    def _execute_program(self, source: str, accumulate: bool, silent: bool):
        """Run a Nemo program, optionally accumulating it into the session.

        Accumulation concatenates the sources of all successful cells and
        re-reasons the whole program (the bindings have no incremental API).
        Identical re-runs are skipped, and failed cells are not accumulated.
        """
        if accumulate:
            parts = list(self._accumulated)
            if source not in parts:
                parts.append(source)
            full = "\n\n".join(parts)
        else:
            full = source

        result = run_program(full)
        if result.ok:
            if accumulate and source not in self._accumulated:
                self._accumulated.append(source)
                result.cell_count = len(self._accumulated)
            elif not accumulate:
                result.cell_count = 0
            self._last = result
            if not silent:
                self._stream(self._format_result(result))
            return self._reply("ok")

        if not silent:
            self._send_error("NemoError", result.error)
        return self._reply("error", "NemoError", result.error)

    def _match_magic(self, text: str):
        """Return (name, argument) if the whole cell is one magic invocation."""
        match = re.match(r"^!(\w+)\s*(.*)$", text, re.DOTALL)
        if match and match.group(1) in _MAGICS:
            return match.group(1), match.group(2).strip()
        return None

    # -- magics ----------------------------------------------------- #

    def _run_magic(self, name: str, arg: str) -> str:
        if name == "help":
            return textwrap.dedent(_HELP).strip()
        if name == "version":
            return (
                f"kernel: nemo_jupyter {self.implementation_version}\n"
                f"bindings: nmo_python {_nemo_version()}\n"
                f"ipykernel: {__import__('ipykernel').__version__}"
            )
        if name == "pwd":
            return os.getcwd()
        if name == "load":
            if not arg:
                raise ValueError("usage: !load <file>")
            with open(arg, encoding="utf-8") as handle:
                return handle.read()
        if name == "predicates":
            if self._last is None:
                return "no program executed yet in this session"
            exported = ", ".join(self._last.predicates) or "(none)"
            imported = ", ".join(parse_import_predicates(self._last.source)) or "(none)"
            return f"exported: {exported}\nimported: {imported}"
        if name == "program":
            if not self._accumulated:
                return "no cells accumulated yet"
            header = f"# accumulated program ({len(self._accumulated)} cells)"
            return header + "\n\n" + "\n\n".join(self._accumulated)
        if name == "reset":
            self._accumulated = []
            self._last = None
            return "accumulated program cleared (0 cells)"
        if name == "standalone":
            raise ValueError("put !standalone on the first line of a cell, followed by the program")
        if name == "trace":
            if not arg:
                raise ValueError("usage: !trace <fact>, e.g. !trace ancestor(ada, bob)")
            if self._last is None or self._last.engine is None:
                return "no program executed yet in this session"
            trace = self._last.engine.trace(arg)
            if trace is None:
                return f"no derivation found for: {arg}"
            return "\n".join(self._format_trace(trace))
        raise ValueError(f"unknown magic: !{name}")

    # -- output formatting ------------------------------------------ #

    def _format_result(self, result: ProgramResult) -> str:
        lines = [result.text]
        summary_parts = []
        for predicate in result.predicates:
            summary_parts.append(f"{predicate}: {result.counts[predicate]}")
        if result.predicates:
            total = sum(result.counts.values())
            summary_parts.append(f"total: {total}")
        if result.timing is not None:
            try:
                ms = result.timing.process_time.total_seconds() * 1000
                summary_parts.append(f"reasoning: {ms:.1f} ms")
            except Exception:
                pass
        if result.cell_count >= 2:
            summary_parts.append(f"cells: {result.cell_count}")
        if summary_parts:
            lines.append("[" + ", ".join(summary_parts) + "]")
        return "\n".join(lines)

    def _format_trace(self, trace, indent: int = 0) -> list[str]:
        pad = "  " * indent
        fact = trace.fact()
        if fact is not None:
            return [pad + str(fact)]
        lines = []
        rule = trace.rule() or "rule"
        assignment = trace.assignement() or {}
        parts = ", ".join(f"{k} = {format_value(v)}" for k, v in assignment.items())
        lines.append(pad + f"[{rule}]" + (f"  ({parts})" if parts else ""))
        for sub in trace.subtraces() or []:
            lines.extend(self._format_trace(sub, indent + 1))
        return lines

    # -- kernel protocol -------------------------------------------- #

    def do_complete(self, code, cursor_pos):
        token_start = cursor_pos
        while token_start > 0 and (code[token_start - 1].isalnum() or code[token_start - 1] == "_"):
            token_start -= 1
        token = code[token_start:cursor_pos]
        matches = sorted(k for k in _COMPLETIONS if k.startswith(token))
        return {
            "matches": matches,
            "cursor_start": token_start,
            "cursor_end": cursor_pos,
            "metadata": {},
            "status": "ok",
        }

    def do_is_complete(self, code):
        return {"status": "complete"}

    def do_shutdown(self, restart):
        return {"status": "ok", "restart": restart}

    # -- low-level message helpers ---------------------------------- #

    def _stream(self, text: str):
        self.send_response(self.iopub_socket, "stream", {"name": "stdout", "text": text + "\n"})

    def _send_error(self, ename: str, evalue: str):
        self.send_response(
            self.iopub_socket,
            "error",
            {"ename": ename, "evalue": evalue, "traceback": [f"{ename}: {evalue}"]},
        )

    def _reply(self, status: str, ename: str = "", evalue: str = ""):
        reply = {
            "status": status,
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }
        if status == "error":
            reply["ename"] = ename
            reply["evalue"] = evalue
            reply["traceback"] = [f"{ename}: {evalue}"]
        return reply

_COMPLETIONS = [
    "@base",
    "@declare",
    "@export",
    "@external",
    "@import",
    "@prefix",
    "DISTINCT",
    "FILTER",
    "NOT",
    "SELECT",
    "WHERE",
    "avg",
    "collect",
    "count",
    "csv",
    "dsv",
    "false",
    "max",
    "min",
    "nquads",
    "ntriples",
    "rdf",
    "rdfxml",
    "sparql",
    "sum",
    "trig",
    "true",
    "tsv",
    "turtle",
]
