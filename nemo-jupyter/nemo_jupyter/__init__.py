"""Nemo Jupyter kernel: run Nemo rule programs in notebooks."""

__version__ = "0.1.0"

from .kernel import NemoKernel, run_program  # noqa: F401

__all__ = ["NemoKernel", "run_program", "__version__"]
