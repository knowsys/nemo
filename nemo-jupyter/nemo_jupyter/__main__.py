"""Entry point: ``python -m nemo_jupyter -f {connection_file}``."""

from ipykernel.kernelapp import IPKernelApp

from .kernel import NemoKernel

IPKernelApp.launch_instance(kernel_class=NemoKernel)
