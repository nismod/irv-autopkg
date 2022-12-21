"""
Generates and Validates Boundary directory structure
"""
import json
import os
import logging

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc.processors.internal.base import BaseProcessorABC
from dataproc import Boundary


class BoundaryProcessor(BaseProcessorABC):
    """Top Level Boundary Structure / Project Setup Processor"""

    index_filename = "index.html"
    version_filename = "version.html"
    license_filename = "license.html"
    datapackage_filename = "datapackage.json"

    def __init__(
        self,
        boundary: dict,
        storage_backend: dict,
        processing_backend: dict,
    ) -> None:
        """
        Boundary Processor initialised with a boundary, storage and processing backends

        NOTE: Init vars are Dictionaries because the Base-class has to inherit from dict for Celery to serialise

        ::param boundary dict Definition of the boundary
        ::param storage_backend dict Storage backend
        ::param processing_backend dict A backend used for processing (tmp file storage etc)
        """
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend
        self.log = logging.getLogger(__name__)

    def generate(self):
        """Generate files for a given processor"""
        # Check Boundary folder exists and generate if not
        if not self.storage_backend.boundary_folder_exists(self.boundary["name"]):
            self.storage_backend.create_boundary_folder(self.boundary["name"])
            self.log.debug("Boundary folder created: True")
        else:
            self.log.debug("Boundary folder exists")
        if not self.storage_backend.boundary_data_folder_exists(self.boundary["name"]):
            self.storage_backend.create_boundary_data_folder(self.boundary["name"])
            self.log.debug("Boundary data folder created: True")
        else:
            self.log.debug("Boundary data folder exists")
        # Generate missing files for the boundary
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.index_filename
        ):
            # Create the index
            index_fpath = self._generate_index_file()
            index_create = self.storage_backend.put_boundary_data(
                index_fpath, self.boundary["name"]
            )
            self.log.debug("Boundary index created: %s", index_create)
        else:
            self.log.debug("Boundary index exists")
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.license_filename
        ):
            license_fpath = self._generate_license_file()
            license_create = self.storage_backend.put_boundary_data(
                license_fpath, self.boundary["name"]
            )
            self.log.debug("Boundary license created: %s", license_create)
        else:
            self.log.debug("Boundary license exists")
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.version_filename
        ):
            version_fpath = self._generate_version_file()
            version_create = self.storage_backend.put_boundary_data(
                version_fpath, self.boundary["name"]
            )
            self.log.debug("Boundary version created: %s", version_create)
        else:
            self.log.debug("Boundary version exists")
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.datapackage_filename
        ):
            datapkg_fpath = self._generate_datapackage_file()
            datapkg_create = self.storage_backend.put_boundary_data(
                datapkg_fpath, self.boundary["name"]
            )
            self.log.debug("Boundary datapackage created: %s", datapkg_create)
        else:
            self.log.debug("Boundary datapackage exists")

    def _generate_index_file(self) -> str:
        """
        Generate the index documentation file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        # Create the file locally
        dest_fpath = os.path.join(
            self.processing_backend.top_level_folder_path, "index.html"
        )
        with open(dest_fpath, "w") as fptr:
            # Insert some content
            fptr.writelines(
                [
                    f"<!doctype html><html><b>Documentation for {self.boundary['name']} Boundary</b></html>"
                ]
            )
        # Return the path
        return dest_fpath

    def _generate_license_file(self) -> str:
        """
        Generate the License documentation file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        # Create the file locally
        dest_fpath = os.path.join(
            self.processing_backend.top_level_folder_path, "license.html"
        )
        with open(dest_fpath, "w") as fptr:
            # Insert some content
            fptr.writelines(
                [
                    f"<!doctype html><html><b>License for {self.boundary['name']} Boundary</b></html>"
                ]
            )
        # Return the path
        return dest_fpath

    def _generate_version_file(self) -> str:
        """
        Generate the Version documentation file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        # Create the file locally
        dest_fpath = os.path.join(
            self.processing_backend.top_level_folder_path, "version.html"
        )
        with open(dest_fpath, "w") as fptr:
            # Insert some content
            fptr.writelines(
                [
                    f"<!doctype html><html><b>Version for {self.boundary['name']} Boundary is 1</b></html>"
                ]
            )
        # Return the path
        return dest_fpath

    def _generate_datapackage_file(self) -> str:
        """
        Generate the Datapackage.json file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        # Create the file locally
        dest_fpath = os.path.join(
            self.processing_backend.top_level_folder_path, "datapackage.json"
        )
        with open(dest_fpath, "w") as fptr:
            # Insert some content
            datapackage = {
                "name": self.boundary["name"],
                "title": self.boundary["name"],
                "profile": f"{self.boundary['name']}-data-package",
                "licenses": [],
                "resources": []
            }
            json.dump(datapackage, fptr)
        # Return the path
        return dest_fpath
