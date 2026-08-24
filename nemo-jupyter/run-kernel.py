#!/usr/bin/env python
"""Launch the Nemo Jupyter kernel.

Used by the kernelspec in ``kernel.json``. Adds this directory to
``sys.path`` so the ``nemo_jupyter`` package is found without installing
it, then starts the kernel with the connection file passed by Jupyter.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ipykernel.kernelapp import IPKernelApp  # noqa: E402
from nemo_jupyter.kernel import NemoKernel  # noqa: E402

IPKernelApp.launch_instance(kernel_class=NemoKernel)
