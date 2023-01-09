"""
Manages provenance files for a given boundary
"""
import os

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC


class ProvenanceProcessor(BaseProcessorABC):
    """Management of Provenance files for a boundary"""

    provenance_log_filename = "provenance.json"

    def __init__(
        self,
        boundary: Boundary,
        storage_backend: StorageBackend,
        processing_backend: ProcessingBackend,
    ) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend

    def generate(self, processing_log: dict):
        """Generate files for a given processor"""
        return self._update_boundary_provenance(processing_log)

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""

    def _update_boundary_provenance(self, processing_log: dict) -> bool:
        """
        Update a given provenance file for a boundary
        """
        return self.storage_backend.add_provenance(
            self.boundary["name"], processing_log, self.provenance_log_filename
        )
