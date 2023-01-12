"""
Manages provenance files for a given boundary
"""
import os
from typing import Any, List
from datetime import datetime

from dataproc.backends import StorageBackend
from dataproc import Boundary


class ProvenanceProcessor():
    """Management of Provenance files for a boundary"""

    provenance_log_filename = "provenance.json"

    def __init__(
        self,
        boundary: Boundary,
        storage_backend: StorageBackend
    ) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend

    def generate(self, processing_log: List[dict]) -> dict:
        """Generate files for a given processor"""
        # Rewmove duplicate boundary processor entries and flatten log
        boundary_log = None
        for log in processing_log:
            if not boundary_log:
                boundary_log = {'boundary_processor':log['boundary_processor']}
            log.pop('boundary_processor')
        processing_log.insert(0, boundary_log)
        _ = self._update_boundary_provenance(processing_log)
        # Return the Prov log as task output
        return {datetime.utcnow().isoformat() : processing_log}

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""

    def _update_boundary_provenance(self, processing_log: List[dict]) -> dict:
        """
        Update a given provenance file for a boundary
        """
        # Add log about provenance running
        processing_log.append({"provenance_processor": {"updated in backend" : True}})
        # Dump the Prov log to backend
        return self.storage_backend.add_provenance(
            self.boundary["name"], processing_log, self.provenance_log_filename
        )

