"""
Manages provenance files for a given boundary
"""
import os
from typing import Any, List
from datetime import datetime
import logging

from dataproc.processors.internal import base
from dataproc.storage import StorageBackend
from dataproc import Boundary, DataPackageResource


class ProvenanceProcessor:
    """Management of Provenance files for a boundary"""

    provenance_log_filename = "provenance.json"

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.log = logging.getLogger(__name__)

    def generate(self, processing_log: List[dict]) -> dict:
        """Generate files for a given processor"""
        # Remove duplicate boundary processor entries and extract DataPackage Update Entries
        boundary_log = None
        datapackage_resources = []
        for log in processing_log:
            if not boundary_log:
                boundary_log = {"boundary_processor": log["boundary_processor"]}
            log.pop("boundary_processor")
            # Extract the datapackage from proevnance log
            for processor_name_version, name_version_log in log.items():
                if "datapackage" in name_version_log.keys():
                    datapackage_resources.append(name_version_log["datapackage"])
        processing_log.insert(0, boundary_log)

        # Update the datapackage
        self._update_datapackage(datapackage_resources)

        # Flatten Log
        _ = self._update_boundary_provenance(processing_log)
        # Return the Prov log as task output
        return {datetime.utcnow().isoformat(): processing_log}

    def _update_boundary_provenance(self, processing_log: List[dict]) -> dict:
        """
        Update a given provenance file for a boundary
        """
        # Add log about provenance running
        processing_log.append({"provenance_processor": {"updated in backend": True}})
        # Dump the Prov log to backend
        return self.storage_backend.add_provenance(
            self.boundary["name"], processing_log, self.provenance_log_filename
        )

    def _update_datapackage(self, resources: List[dict]) -> None:
        """
        Update a packages datapackage.json resource and license entries
            with information from the processor which has run before the prov. processor
        """
        for resource in resources:
            try:
                # Convert back to DPResource and License
                dp_license = base.DataPackageLicense(
                    name=resource["license"]["name"],
                    path=resource["license"]["path"],
                    title=resource["license"]["title"],
                )
                dp_resource = DataPackageResource(
                    name=resource["name"],
                    version=resource["version"],
                    path=resource["path"],
                    description=resource["description"],
                    dataset_format=resource["format"],
                    dataset_size_bytes=resource["bytes"],
                    dataset_hashes=resource["hashes"],
                    sources=resource["sources"],
                    dp_license=dp_license,
                )
                self.storage_backend.update_datapackage(
                    self.boundary["name"], dp_resource
                )
            except Exception as err:
                self.log.error(
                    "Failed to update datapackage.json for %s with resource: %s, due to: %s",
                    self.boundary["name"],
                    resource,
                    err,
                )
