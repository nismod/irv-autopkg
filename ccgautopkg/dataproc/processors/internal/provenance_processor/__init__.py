"""
Manages provenance files for a given boundary
"""

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC


class ProvenanceProcessor(BaseProcessorABC):
    """Management of Provenance files for a boundary"""

    def __init__(
        self,
        boundary: Boundary,
        storage_backend: StorageBackend,
        processing_backend: ProcessingBackend,
    ) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend

    def generate(self, previous_result_sink):
        """Generate files for a given processor"""
        return previous_result_sink

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
