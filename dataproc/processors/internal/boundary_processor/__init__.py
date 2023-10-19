"""
Generates and Validates Boundary directory structure
"""
import json
import os
import logging

from config import LOCALFS_PROCESSING_BACKEND_ROOT
from dataproc.storage.base import StorageBackend


class BoundaryProcessor:
    """Top Level Boundary Structure / Project Setup Processor"""

    index_filename = "index.html"
    version_filename = "version.html"
    license_filename = "license.html"
    version_filename = "version.html"
    datapackage_filename = "datapackage.json"

    def __init__(self, boundary: dict, storage_backend: StorageBackend) -> None:
        """
        Boundary Processor initialised with a boundary, storage and processing backends

        NOTE: Init vars are Dictionaries because the Base-class has to inherit from dict for Celery to serialise

        ::param boundary dict Definition of the boundary
        ::param storage_backend dict Storage backend
        """
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.log = logging.getLogger(__name__)
        self.provenance_log = {}
        if LOCALFS_PROCESSING_BACKEND_ROOT is None:
            raise KeyError("LOCALFS_PROCESSING_BACKEND_ROOT not loaded from config")
        self.tmp_processing_folder = os.path.join(
            LOCALFS_PROCESSING_BACKEND_ROOT,
            "boundary_processor",
            str(self.boundary["name"]),
            "tmp",
        )
        os.makedirs(self.tmp_processing_folder, exist_ok=True)

    def generate(self) -> dict:
        """Generate files for a given processor"""
        # Check Boundary folder exists and generate if not
        if not self.storage_backend.boundary_folder_exists(self.boundary["name"]):
            self.storage_backend.create_boundary_folder(self.boundary["name"])
            self.log.debug(
                "Boundary folder for %s created: True", self.boundary["name"]
            )
            self.provenance_log["boundary_folder"] = "created"
        else:
            self.log.debug("Boundary for %s folder exists", self.boundary["name"])
            self.provenance_log["boundary_folder"] = "exists"
        if not self.storage_backend.boundary_data_folder_exists(self.boundary["name"]):
            self.storage_backend.create_boundary_data_folder(self.boundary["name"])
            self.log.debug(
                "Boundary data folder for %s created: True", self.boundary["name"]
            )
            self.provenance_log["boundary_data_folder"] = "created"
        else:
            self.log.debug("Boundary data folder for %s exists", self.boundary["name"])
        # Generate missing files for the boundary
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.index_filename
        ):
            # Create the index
            index_fpath = self._generate_index_file()
            index_create = self.storage_backend.put_boundary_data(
                index_fpath, self.boundary["name"]
            )
            self.log.debug(
                "Boundary index for %s created: %s", self.boundary["name"], index_create
            )
            self.provenance_log["boundary_index"] = "created"
        else:
            self.log.debug("Boundary index for %s exists", self.boundary["name"])
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.license_filename
        ):
            license_fpath = self._generate_license_file()
            license_create = self.storage_backend.put_boundary_data(
                license_fpath, self.boundary["name"]
            )
            self.log.debug(
                "Boundary license for %s created: %s",
                self.boundary["name"],
                license_create,
            )
            self.provenance_log["boundary_license"] = "created"
        else:
            self.log.debug("Boundary license for %s exists", self.boundary["name"])
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.version_filename
        ):
            version_fpath = self._generate_version_file()
            version_create = self.storage_backend.put_boundary_data(
                version_fpath, self.boundary["name"]
            )
            self.log.debug(
                "Boundary version for %s created: %s",
                self.boundary["name"],
                version_create,
            )
            self.provenance_log["boundary_version"] = "created"
        else:
            self.log.debug("Boundary version for %s exists", self.boundary["name"])
        if not self.storage_backend.boundary_file_exists(
            self.boundary["name"], self.datapackage_filename
        ):
            datapkg_fpath = self._generate_datapackage_file()
            datapkg_create = self.storage_backend.put_boundary_data(
                datapkg_fpath, self.boundary["name"]
            )
            self.log.debug(
                "Boundary datapackage for %s created: %s",
                self.boundary["name"],
                datapkg_create,
            )
            self.provenance_log["boundary_datapackage"] = "created"
        else:
            self.log.debug("Boundary datapackage for %s exists", self.boundary["name"])
        return {"boundary_processor": self.provenance_log}

    def _generate_index_file(self) -> str:
        """
        Generate the index documentation file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        template_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "templates", self.index_filename
        )
        return template_fpath

    def _generate_license_file(self) -> str:
        """
        Generate the License documentation file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        template_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            self.license_filename,
        )
        return template_fpath

    def _generate_version_file(self) -> str:
        """
        Generate the Version documentation file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        template_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            self.version_filename,
        )
        return template_fpath

    def _generate_datapackage_file(self) -> str:
        """
        Generate the Datapackage.json file for a boundary

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        ## TODO extract template - live with storage backend?? or processing root/base?
        template_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            self.datapackage_filename,
        )
        dest_fpath = os.path.join(self.tmp_processing_folder, "datapackage.json")
        with open(template_fpath, "r") as fptr:
            datapkg = json.load(fptr)
        # Insert some content
        datapkg["name"] = self.boundary["name"]
        datapkg["title"] = self.boundary["name"]
        with open(dest_fpath, "w") as fptr:
            json.dump(datapkg, fptr)
            # Return the path
            return dest_fpath
