

from abc import ABC

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc import Boundary

class BaseMetadataABC(ABC):
    """Base Metadata ABC"""

class BaseProcessorABC(ABC):
    """Base Processor ABC"""

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend, processing_backend: ProcessingBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend
        self.provenance_log = {}

    def generate(self):
        """Generate files for a given processor"""

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
