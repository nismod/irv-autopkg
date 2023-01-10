"""
Test Raster Processor
"""

import os
import logging
import zipfile

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC

class Metadata(BaseMetadataABC):
    """Processor metadata"""
    name="natural_earth" # this must follow snakecase formatting, without special chars
    description="A Test Processor for Natural Earth image" # Longer processor description
    version="1" # Version of the Processor
    dataset_name="natural_earth" # The dataset this processor targets
    data_author="Natural Earth Data"
    data_license="https://www.naturalearthdata.com/about/terms-of-use/"
    data_origin_url="https://www.naturalearthdata.com/downloads/50m-natural-earth-2/50m-natural-earth-ii-with-shaded-relief/"

class Processor(BaseProcessorABC):
    """A Processor for _two"""

    source_zip_filename = "NE2_50M_SR.zip"
    source_zip_url = os.path.join("https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/raster/NE2_50M_SR.zip")

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend, processing_backend: ProcessingBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend
        self.provenance_log = {}
        self.log = logging.getLogger(__name__)

    def exists(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        return False

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            self.provenance_log[Metadata().name] = "exists"
            return self.provenance_log
        # Check if the source TIFF exists and fetch it if not
        geotiff_fpath = self._fetch_source()
        # Crop to given boundary
        output_folder = self.processing_backend.create_processing_folder(f"natural_earth/{Metadata().version}/outputs")
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}.tif")
        self.log.debug("Natural earth - cropping geotiff")
        crop_success = self.processing_backend.crop_raster(geotiff_fpath, output_fpath, self.boundary)
        self.provenance_log[f"{Metadata().name} - crop success"] = crop_success
        # Move cropped data to backend
        self.log.debug("Natural earth - moving cropped data to backend")
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary['name'],
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
        expected_source_path = self.processing_backend.build_absolute_path("natural_earth", "NE2_50M_SR", "NE2_50M_SR.tif")
        if self.processing_backend.path_exists(expected_source_path):
            self.provenance_log[f"{Metadata().name} - source files exist"] = expected_source_path
            self.log.debug("Natural earth - source exists")
            return expected_source_path
        # Fetch the source zip
        self.log.debug("Natural earth - fetching zip")
        local_zip_fpath = self._fetch_zip()
        self.log.debug("Natural earth - fetched zip to %s", local_zip_fpath)
        self.provenance_log[f"{Metadata().name} - zip download path"] = local_zip_fpath
        # Unpack
        self.log.debug("Natural earth - unpacking zip")
        local_extract_path = self.processing_backend.unpack_zip(local_zip_fpath)
        geotiff_fpath = os.path.join(local_extract_path, "NE2_50M_SR.tif")
        assert os.path.exists(geotiff_fpath), f"extracted GTIFF did not exist at expected path {geotiff_fpath}"
        # Assert the Tiff
        self.log.debug("Natural earth - asserting geotiff")
        self.processing_backend.assert_geotiff(geotiff_fpath, check_crs='EPSG:4326')
        self.provenance_log[f"{Metadata().name} - valid GeoTIFF fetched/extracted"] = True
        return geotiff_fpath

    def _fetch_zip(self) -> str:
        """
        Fetch the Source Zip File

        ::returns filepath str Result local filepath
        """
        # Pull the zip file to the configured processing backend
        zip_path = self.processing_backend.download_file(self.source_zip_url, "natural_earth/NE2_50M_SR.zip")
        return zip_path

    