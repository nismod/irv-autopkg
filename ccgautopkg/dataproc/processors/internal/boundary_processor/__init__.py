"""
Generates and Validates Boundary directory structure
"""

from dataproc.backends import Backend
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import Boundary

class BoundaryProcessor(BaseProcessorABC):
    """Top Level Boundary Structure / Project Setup Processor"""

    def __init__(self, boundary: Boundary, backend: Backend) -> None:
        self.boundary = boundary
        self.backend = backend

    def generate(self):
        """Generate files for a given processor"""
        return True

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
