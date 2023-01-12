"""
Test Raster Processor
"""

import os
import logging
import inspect

from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    version_name_from_file,
    crop_raster,
    unpack_zip,
    assert_geotiff,
    download_file,
)
from config import LOCALFS_PROCESSING_BACKEND_ROOT


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = "test_natural_earth_raster"  # this must follow snakecase formatting, without special chars
    description = (
        "A Test Processor for Natural Earth image"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "test_natural_earth_raster"  # The dataset this processor targets
    data_author = "Natural Earth Data"
    data_license = "https://www.naturalearthdata.com/about/terms-of-use/"
    data_origin_url = "https://www.naturalearthdata.com/downloads/50m-natural-earth-2/50m-natural-earth-ii-with-shaded-relief/"


class Processor(BaseProcessorABC):
    """A Processor for Natural Earth"""

    source_zip_filename = "NE2_50M_SR.zip"
    source_zip_url = os.path.join(
        "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/raster/NE2_50M_SR.zip"
    )

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.paths_helper = PathsHelper(os.path.join(LOCALFS_PROCESSING_BACKEND_ROOT, Metadata().name))
        self.provenance_log = {}
        self.log = logging.getLogger(__name__)

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
            "test_natural_earth_raster",
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
        # Cleanup as required
        return self.provenance_log

    def _fetch_source(self) -> str:
        """
        Fetch and unpack the required source data if required.
            Returns the path to existing files if they already exist

        return fpath str The path to the fetch source file
        """
        # Check if the files exist
        expected_source_path = self.paths_helper.build_absolute_path(
            "test_natural_earth_raster", "NE2_50M_SR", "NE2_50M_SR.tif"
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
        local_extract_path = unpack_zip(local_zip_fpath)
        geotiff_fpath = os.path.join(local_extract_path, "NE2_50M_SR.tif")
        assert os.path.exists(
            geotiff_fpath
        ), f"extracted GTIFF did not exist at expected path {geotiff_fpath}"
        # Assert the Tiff
        self.log.debug("Natural earth raster - asserting geotiff")
        assert_geotiff(geotiff_fpath, check_crs="EPSG:4326")
        self.provenance_log[
            f"{Metadata().name} - valid GeoTIFF fetched/extracted"
        ] = True
        return geotiff_fpath

    def _fetch_zip(self) -> str:
        """
        Fetch the Source Zip File

        ::returns filepath str Result local filepath
        """
        # Pull the zip file to the configured processing backend
        zip_path = download_file(
            self.source_zip_url,
            self.paths_helper.build_absolute_path(
                "test_natural_earth_raster", "NE2_50M_SR.zip"
            ),
        )
        return zip_path
