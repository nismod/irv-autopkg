"""
Test Raster Processor
"""

from time import sleep

from dataproc.backends import Backend
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC

class RasterProcessorTwo(BaseProcessorABC):
    """A Raster Processor"""

    def __init__(self, boundary: Boundary, backend: Backend) -> None:
        self.boundary = boundary
        self.backend = backend

    def generate(self):
        """Generate files for a given processor"""
        sleep(1)
        return True

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
