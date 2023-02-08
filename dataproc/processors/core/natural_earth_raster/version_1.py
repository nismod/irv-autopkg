"""
Test Raster Processor
"""

import os
import logging
import inspect
import shutil
from typing import List

from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary, DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    processor_name_from_file,
    version_name_from_file,
    crop_raster,
    unpack_zip,
    assert_geotiff,
    download_file,
    datapackage_resource,
    data_file_hash,
    data_file_size
)
from config import LOCALFS_PROCESSING_BACKEND_ROOT


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(inspect.stack()[1].filename)  # this must follow snakecase formatting, without special chars
    description = (
        "A Test Processor for Natural Earth image"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "natural_earth_raster"  # The dataset this processor targets
    data_author = "Natural Earth Data"
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://www.naturalearthdata.com/downloads/50m-natural-earth-2/50m-natural-earth-ii-with-shaded-relief/"


class Processor(BaseProcessorABC):
    """A Processor for Natural Earth"""

    index_filename = "index.html"
    license_filename = "license.html"
    source_zip_filename = "NE2_50M_SR.zip"
    source_zip_url = os.path.join(
        "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/raster/NE2_50M_SR.zip"
    )

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        """"""
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.paths_helper = PathsHelper(
            os.path.join(LOCALFS_PROCESSING_BACKEND_ROOT, Metadata().name, Metadata().version)
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
            f"{self.boundary['name']}.tif",
        )

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            self.provenance_log[Metadata().name] = "exists"
            return self.provenance_log
        # Check if the source TIFF exists and fetch it if not
        geotiff_fpath = self._fetch_source()
        # Crop to given boundary
        output_folder = self.paths_helper.build_absolute_path(
            self.boundary["name"],
            "natural_earth_raster",
            Metadata().version,
            "outputs",
        )
        os.makedirs(output_folder, exist_ok=True)

        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}.tif")
        self.log.debug("Natural earth raster - cropping geotiff")
        crop_success = crop_raster(geotiff_fpath, output_fpath, self.boundary)
        self.provenance_log[f"{Metadata().name} - crop success"] = crop_success
        # Move cropped data to backend
        self.log.debug("Natural earth raster - moving cropped data to backend")
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

        return self.provenance_log

    def _fetch_source(self) -> str:
        """
        Fetch and unpack the required source data if required.
            Returns the path to existing files if they already exist

        return fpath str The path to the fetch source file
        """
        # Check if the files exist
        expected_source_path = self.paths_helper.build_absolute_path(
            "natural_earth_raster", "NE2_50M_SR", "NE2_50M_SR.tif"
        )
        if os.path.exists(expected_source_path):
            self.provenance_log[
                f"{Metadata().name} - source files exist"
            ] = expected_source_path
            self.log.debug("Natural earth raster - source exists")
            return expected_source_path
        # Fetch the source zip
        self.log.debug("Natural earth raster - fetching zip")
        local_zip_fpath = self._fetch_zip()
        self.log.debug("Natural earth raster - fetched zip to %s", local_zip_fpath)
        self.provenance_log[f"{Metadata().name} - zip download path"] = local_zip_fpath
        # Unpack
        self.log.debug("Natural earth raster - unpacking zip")
        unpack_zip(local_zip_fpath, self.source_folder)
        # Zip gets unpacked to a nested directory for this source
        geotiff_fpath = os.path.join(self.source_folder, "NE2_50M_SR", "NE2_50M_SR.tif")
        assert os.path.exists(
            geotiff_fpath
        ), f"extracted GTIFF did not exist at expected path {geotiff_fpath}"
        # Assert the Tiff
        self.log.debug("Natural earth raster - asserting geotiff")
        assert_geotiff(geotiff_fpath, check_crs="EPSG:4326", check_compression=False)
        self.provenance_log[
            f"{Metadata().name} - valid GeoTIFF fetched/extracted"
        ] = True
        return geotiff_fpath

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
        self.log.debug("Natural Earth Raster generated documentation on backend")

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

    def _fetch_zip(self) -> str:
        """
        Fetch the Source Zip File

        ::returns filepath str Result local filepath
        """
        # Pull the zip file to the configured processing backend
        zip_path = download_file(
            self.source_zip_url,
            self.paths_helper.build_absolute_path(
                "natural_earth_raster", "NE2_50M_SR.zip"
            ),
        )
        return zip_path
