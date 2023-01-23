"""
Vector Processor for OSM (inc. damages)
"""

import os
import logging
import inspect
import shutil

from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    version_name_from_file,
    gdal_crop_pg_table_to_geopkg
)
from config import (
    LOCALFS_PROCESSING_BACKEND_ROOT,
    get_db_uri_ogr
)


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = "gri_osm"  # this must follow snakecase formatting, without special chars
    description = (
        "Extraction from GRI OSM Tables, including Damages"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = (
        "gri_osm"  # The dataset this processor targets
    )
    data_author = "GRI/OSM"
    data_license = "TBD"
    data_origin_url = (
        "https://global.infrastructureresilience.org"
    )


class Processor(BaseProcessorABC):
    """A Processor for GRI OSM Database"""

    index_filename = "index.html"
    license_filename = "license.html"

    pg_osm_host_env = "CCGAUTOPKG_OSM_PGHOST"
    pg_osm_port_env = "CCGAUTOPKG_OSM_PORT"
    pg_osm_user_env = "CCGAUTOPKG_OSM_PGUSER"
    pg_osm_password_env = "CCGAUTOPKG_OSM_PGPASSWORD"
    pg_osm_dbname_env = "CCGAUTOPKG_OSM_PGDATABASE"
    input_pg_table = "features"
    input_geometry_column = "geom"
    output_geometry_operation = "clip" # Clip or intersect
    output_geometry_column = "clipped_geometry"
    output_layer_name = "gri-osm"

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.paths_helper = PathsHelper(LOCALFS_PROCESSING_BACKEND_ROOT)
        self.provenance_log = {}
        self.log = logging.getLogger(__name__)
        # Source folder will persist between processor runs
        self.source_folder = self.paths_helper.build_absolute_path("source_data")
        os.makedirs(self.source_folder, exist_ok=True)
        # Tmp Processing data will be cleaned between processor runs
        self.tmp_processing_folder = self.paths_helper.build_absolute_path("tmp")
        os.makedirs(self.tmp_processing_folder, exist_ok=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup any resources as required"""
        self.log.debug(
            "cleaning processing data on exit, exc: %s, %s, %s",
            exc_type,
            exc_val,
            exc_tb,
        )
        try:
            shutil.rmtree(self.tmp_processing_folder)
        except FileNotFoundError:
            pass

    def exists(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        return self.storage_backend.processor_file_exists(
            self.boundary["name"],
            Metadata().name,
            Metadata().version,
            f"{self.boundary['name']}.gpkg",
        )

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            self.provenance_log[Metadata().name] = "exists"
            return self.provenance_log
        # Setup output path in the processing backend
        output_folder = self.paths_helper.build_absolute_path(
            self.boundary["name"], Metadata().name, Metadata().version, "outputs"
        )
        os.makedirs(output_folder, exist_ok=True)
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}.gpkg")

        # Crop to given boundary
        self.log.debug("%s - cropping to geopkg", Metadata().name)
        gdal_crop_pg_table_to_geopkg(
            self.boundary,
            str(get_db_uri_ogr(
                dbname=os.getenv(self.pg_osm_dbname_env),
                username_env=self.pg_osm_user_env,
                password_env=self.pg_osm_password_env,
                host_env=self.pg_osm_host_env,
                port_env=self.pg_osm_port_env
            )),
            self.input_pg_table,
            output_fpath,
            geometry_column=self.input_geometry_column,
            extract_type=self.output_geometry_operation,
            clipped_geometry_column_name=self.output_geometry_column
        )
        self.provenance_log[f"{Metadata().name} - crop completed"] = True
        # Move cropped data to backend
        self.log.debug("%s - moving cropped data to backend", {Metadata().name})
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary["name"],
            Metadata().name,
            Metadata().version,
        )
        self.provenance_log[f"{Metadata().name} - move to storage success"] = True
        self.provenance_log[f"{Metadata().name} - result URI"] = result_uri

        # Generate Documentation
        index_fpath = self._generate_index_file()
        index_create = self.storage_backend.put_boundary_data(
            index_fpath, self.boundary["name"]
        )
        self.provenance_log[f"{Metadata().name} - created index documentation"] = index_create
        license_fpath = self._generate_license_file()
        license_create = self.storage_backend.put_boundary_data(
            license_fpath, self.boundary["name"]
        )
        self.provenance_log[f"{Metadata().name} - created license documentation"] = license_create
        
        # Cleanup as required
        return self.provenance_log

    def _generate_index_file(self) -> str:
        """
        Generate the index documentation file

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        template_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "templates", self.index_filename
        )
        return template_fpath

    def _generate_license_file(self) -> str:
        """
        Generate the License documentation file

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        template_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            self.license_filename,
        )
        return template_fpath