"""
Test vector Processor
"""

import os
import logging
import inspect
import shutil
from typing import List

import sqlalchemy as sa

from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary, DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    processor_name_from_file,
    version_name_from_file,
    unpack_zip,
    download_file,
    ogr2ogr_load_shapefile_to_pg,
    gdal_crop_pg_table_to_geopkg,
    datapackage_resource,
    data_file_hash,
    data_file_size
)
from config import (
    LOCALFS_PROCESSING_BACKEND_ROOT,
    get_db_uri_ogr,
    get_db_uri_sync,
    API_POSTGRES_DB,
)


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(inspect.stack()[1].filename)  # this must follow snakecase formatting, without special chars
    description = (
        "A Test Processor for Natural Earth vector"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = (
        "natural_earth_vector_roads"  # The dataset this processor targets
    )
    data_author = "Natural Earth Data"
    data_license = ""
    data_license = DataPackageLicense(
        name="Natural Earth",
        title="Natural Earth",
        path="https://www.naturalearthdata.com/about/terms-of-use/",
    )
    data_origin_url = (
        "https://www.naturalearthdata.com/downloads/10m-cultural-vectors/roads/"
    )


class Processor(BaseProcessorABC):
    """A Processor for Natural Earth Vector"""

    index_filename = "index.html"
    license_filename = "license.html"
    source_zip_filename = "ne_10m_roads.zip"
    source_zip_url = os.path.join(
        "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_roads.zip"
    )
    input_geometry_column = "wkb_geometry"
    output_geometry_operation = "clip"  # Clip or intersect
    output_geometry_column = "clipped_geometry"

    pg_dbname_env="CCGAUTOPKG_POSTGRES_DB"
    pg_user_env="CCGAUTOPKG_POSTGRES_USER"
    pg_password_env="CCGAUTOPKG_POSTGRES_PASSWORD"
    pg_host_env="CCGAUTOPKG_POSTGRES_HOST"
    pg_port_env="CCGAUTOPKG_POSTGRES_PORT"

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
            str(get_db_uri_ogr(
                dbname=os.getenv(self.pg_dbname_env),
                username_env=self.pg_user_env,
                password_env=self.pg_password_env,
                host_env=self.pg_host_env,
                port_env=self.pg_port_env
            )),
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

        # Generate Datapackage
        hashes = [data_file_hash(output_fpath)]
        sizes = [data_file_size(output_fpath)]
        self.generate_datapackage(
            [result_uri], hashes, sizes
        )

        # Generate Docs
        self.generate_documentation()

        # Cleanup as required
        return self.provenance_log

    def generate_datapackage(self, uris: str, hashes: List[str], sizes: List[int]):
        """Generate the datapackage resource for this processor
        and append to processor log
        """
        # Generate the datapackage and add it to the output log
        datapkg = datapackage_resource(
            Metadata(),
            uris,
            "GEOPKG",
            hashes,
            sizes,
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug("Aqueduct generated datapackage in log: %s", datapkg)

    def generate_documentation(self):
        """Generate documentation for the processor
        on the result backend"""
        index_fpath = self._generate_index_file()
        index_create = self.storage_backend.put_processor_metadata(
            index_fpath, self.boundary["name"],
            Metadata().name,
            Metadata().version,
        )
        self.provenance_log[
            f"{Metadata().name} - created index documentation"
        ] = index_create
        license_fpath = self._generate_license_file()
        license_create = self.storage_backend.put_processor_metadata(
            license_fpath, self.boundary["name"],
            Metadata().name,
            Metadata().version,
        )
        self.provenance_log[
            f"{Metadata().name} - created license documentation"
        ] = license_create
        self.log.debug("Natural Earth Vector generated documentation on backend")

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
        unpack_zip(local_zip_fpath, self.source_folder)
        shp_fpath = os.path.join(
            self.source_folder, "ne_10m_roads.shp"
        )
        assert os.path.exists(
            shp_fpath
        ), f"extracted SHP did not exist at expected path {shp_fpath}"
        # Load to PG
        self.log.debug("Natural earth vector - loading shapefile to PG")
        ogr2ogr_load_shapefile_to_pg(shp_fpath, get_db_uri_ogr(API_POSTGRES_DB))
        self.provenance_log[f"{Metadata().name} - loaded NE Roads to PG"] = True
        return "ne_10m_roads"

    def _fetch_zip(self) -> str:
        """
        Fetch the Source Zip File

        ::returns filepath str Result local filepath
        """
        # Pull the zip file to the configured processing backend
        zip_fpath = self.paths_helper.build_absolute_path(
            "natural_earth_vector", "ne_10m_roads.zip"
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
        db_uri = get_db_uri_sync(API_POSTGRES_DB)
        # Init DB and Load via SA
        engine = sa.create_engine(db_uri, pool_pre_ping=True)
        _ = engine.execute("DROP TABLE IF EXISTS ne_10m_roads;")
