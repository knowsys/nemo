"""nbconvert script exporter for Nemo notebooks.

Registered as the ``nemo`` exporter in the ``nbconvert.exporters.script``
entry point group (see ``pyproject.toml``). This is what makes
"Export as Executable Script" in JupyterLab / VS Code produce a single
``.rls`` file that can be run with the ``nmo`` CLI.

Kernel magics are stripped (``!load`` files are inlined); the exported
file is the concatenation of all code cells — the accumulated program.
"""

from __future__ import annotations

from nbconvert.exporters.templateexporter import TemplateExporter

from .nemo_source import clean_nemo_source

__all__ = ["NemoScriptExporter"]


class NemoScriptExporter(TemplateExporter):
    """Export a Nemo notebook as one ``.rls`` program file."""

    file_extension = ".rls"
    output_mimetype = "text/x-nemo"
    export_from_notebook = "Nemo script"

    def from_notebook_node(self, nb, resources=None, **kw):
        base_dir = None
        if resources:
            path = resources.get("metadata", {}).get("path")
            if path:
                base_dir = str(path)

        parts: list[str] = []
        name = (resources or {}).get("metadata", {}).get("name", "")
        if name:
            parts.append(f"% Exported from notebook: {name}")
        for cell in nb.cells:
            if cell.cell_type != "code":
                continue
            cleaned = clean_nemo_source(cell.source, base_dir=base_dir)
            if cleaned.strip():
                parts.append(cleaned)

        output = "\n\n".join(parts) + "\n"

        resources = resources or {}
        resources["output_extension"] = self.file_extension
        return output, resources
