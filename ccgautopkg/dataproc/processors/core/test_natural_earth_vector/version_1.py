"""
Test vector Processor
"""

import os
import logging
import inspect
import shutil

import sqlalchemy as sa

from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    version_name_from_file,
    unpack_zip,
    download_file,
    ogr2ogr_load_shapefile_to_pg,
    gdal_crop_pg_table_to_geopkg,
)
from config import (
    LOCALFS_PROCESSING_BACKEND_ROOT,
    get_db_uri_ogr,
    get_db_uri_sync,
    API_DB_NAME,
)


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = "test_natural_earth_vector"  # this must follow snakecase formatting, without special chars
    description = (
        "A Test Processor for Natural Earth vector"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = (
        "test_natural_earth_vector_roads"  # The dataset this processor targets
    )
    data_author = "Natural Earth Data"
    data_license = "https://www.naturalearthdata.com/about/terms-of-use/"
    data_origin_url = (
        "https://www.naturalearthdata.com/downloads/10m-cultural-vectors/roads/"
    )


class Processor(BaseProcessorABC):
    """A Processor for Natural Earth Vector"""

    source_zip_filename = "ne_10m_roads.zip"
    source_zip_url = os.path.join(
        "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_roads.zip"
    )
    input_geometry_column = "wkb_geometry"
    output_geometry_operation = "clip"  # Clip or intersect
    output_geometry_column = "clipped_geometry"

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.paths_helper = PathsHelper(
            os.path.join(LOCALFS_PROCESSING_BACKEND_ROOT, Metadata().name)
        )
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
        # Check if the source exists and fetch it if not
        pg_table_name = self._fetch_source()
        # Crop to given boundary
        output_folder = self.paths_helper.build_absolute_path(
            self.boundary["name"], Metadata().name, Metadata().version, "outputs"
        )
        os.makedirs(output_folder, exist_ok=True)
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}.gpkg")

        self.log.debug("Natural earth vector - cropping Roads to geopkg")
        gdal_crop_pg_table_to_geopkg(
            self.boundary,
            str(get_db_uri_ogr("ccgautopkg")),
            pg_table_name,
            output_fpath,
            geometry_column=self.input_geometry_column,
            extract_type=self.output_geometry_operation,
            clipped_geometry_column_name=self.output_geometry_column,
        )
        self.provenance_log[f"{Metadata().name} - crop completed"] = True
        # Move cropped data to backend
        self.log.debug("Natural earth vector - moving cropped data to backend")
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary["name"],
            Metadata().name,
            Metadata().version,
        )
        self.provenance_log[f"{Metadata().name} - move to storage success"] = True
        self.provenance_log[f"{Metadata().name} - result URI"] = result_uri
        # Cleanup as required
        return self.provenance_log

    def _fetch_source(self) -> str:
        """
        Fetch and unpack the required source data if required.
            Returns the path to existing files if they already exist

        return fpath str The path to the fetch source file
        """
        # Drop the source from Db as we are testing
        self.drop_natural_earth_roads_from_pg()
        # Fetch the source zip
        self.log.debug("Natural earth vector - fetching zip")
        local_zip_fpath = self._fetch_zip()
        self.log.debug("Natural earth vector - fetched zip to %s", local_zip_fpath)
        self.provenance_log[f"{Metadata().name} - zip download path"] = local_zip_fpath
        # Unpack
        self.log.debug("Natural earth vector - unpacking zip")
        local_extract_path = unpack_zip(local_zip_fpath)
        shp_fpath = os.path.join(
            os.path.dirname(local_extract_path), "ne_10m_roads.shp"
        )
        assert os.path.exists(
            shp_fpath
        ), f"extracted SHP did not exist at expected path {shp_fpath}"
        # Load to PG
        self.log.debug("Natural earth vector - loading shapefile to PG")
        ogr2ogr_load_shapefile_to_pg(shp_fpath, get_db_uri_ogr("ccgautopkg"))
        self.provenance_log[f"{Metadata().name} - loaded NE Roads to PG"] = True
        return "ne_10m_roads"

    def _fetch_zip(self) -> str:
        """
        Fetch the Source Zip File

        ::returns filepath str Result local filepath
        """
        # Pull the zip file to the configured processing backend
        zip_fpath = self.paths_helper.build_absolute_path(
            "test_natural_earth_vector", "ne_10m_roads.zip"
        )
        if not os.path.exists(zip_fpath):
            zip_fpath = download_file(
                self.source_zip_url,
                zip_fpath,
            )
        return zip_fpath

    @staticmethod
    def drop_natural_earth_roads_from_pg():
        """Drop loaded Natural Earth Roads data from DB"""
        db_uri = get_db_uri_sync(API_DB_NAME)
        # Init DB and Load via SA
        engine = sa.create_engine(db_uri, pool_pre_ping=True)
        _ = engine.execute("DROP TABLE IF EXISTS ne_10m_roads;")
