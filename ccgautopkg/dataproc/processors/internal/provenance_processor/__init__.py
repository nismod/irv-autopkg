"""
Manages provenance files for a given boundary
"""

from dataproc.backends import Backend
from dataproc.helpers import Boundary
from dataproc.processors.internal.base import BaseProcessorABC

class ProvenanceProcessor(BaseProcessorABC):
    """Management of Provenance files for a boundary"""

    def __init__(self, boundary: Boundary, backend: Backend) -> None:
        self.boundary = boundary
        self.backend = backend

    def generate(self):
        """Generate files for a given processor"""
        return True

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""