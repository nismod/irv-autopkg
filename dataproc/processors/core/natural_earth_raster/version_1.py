"""
Test Raster Processor
"""

import os
import inspect

from dataproc import DataPackageLicense
from dataproc.exceptions import ProcessorDatasetExists
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    processor_name_from_file,
    version_name_from_file,
    crop_raster,
    unpack_zip,
    assert_geotiff,
    download_file,
    generate_datapackage,
    generate_index_file,
    generate_license_file,
    data_file_hash,
    data_file_size,
    output_filename,
)


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(
        inspect.stack()[1].filename
    )  # this must follow snakecase formatting, without special chars
    description = (
        "A Test Processor for Natural Earth image"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "natural_earth_raster"  # The dataset this processor targets
    data_author = "Natural Earth Data"
    data_title = ""
    data_title_long = ""
    data_summary = ""
    data_citation = ""
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://www.naturalearthdata.com/downloads/50m-natural-earth-2/50m-natural-earth-ii-with-shaded-relief/"
    data_formats = ["GeoTIFF"]


class Processor(BaseProcessorABC):
    """A Processor for Natural Earth"""

    index_filename = "index.html"
    license_filename = "license.html"
    source_zip_filename = "NE2_50M_SR.zip"
    source_zip_url = os.path.join(
        "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/raster/NE2_50M_SR.zip"
    )

    def exists(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        return self.storage_backend.processor_file_exists(
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
            output_filename(
                self.metadata.name, self.metadata.version, self.boundary["name"], "tif"
            ),
        )

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            raise ProcessorDatasetExists()
        # Check if the source TIFF exists and fetch it if not
        self.update_progress(10, "fetching and verifying source")
        geotiff_fpath = self._fetch_source()
        # Crop to given boundary
        self.update_progress(50, "cropping source")
        output_fpath = os.path.join(
            self.tmp_processing_folder,
            output_filename(
                self.metadata.name, self.metadata.version, self.boundary["name"], "tif"
            ),
        )
        self.log.debug("Natural earth raster - cropping geotiff")
        crop_success = crop_raster(geotiff_fpath, output_fpath, self.boundary)
        self.provenance_log[f"{self.metadata.name} - crop success"] = crop_success
        # Move cropped data to backend
        self.update_progress(80, "moving result")
        self.log.debug("Natural earth raster - moving cropped data to backend")
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
        )
        self.provenance_log[f"{self.metadata.name} - move to storage success"] = True
        self.provenance_log[f"{self.metadata.name} - result URI"] = result_uri

        # Generate Datapackage
        self.update_progress(90, "generate datapackage")
        hashes = [data_file_hash(output_fpath)]
        sizes = [data_file_size(output_fpath)]
        datapkg = generate_datapackage(
            self.metadata, [result_uri], "GeoTIFF", sizes, hashes
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug(
            "%s generated datapackage in log: %s", self.metadata.name, datapkg
        )

        # Generate Docs
        self.update_progress(100, "generate documentation")
        self.generate_documentation()

        return self.provenance_log

    def generate_documentation(self):
        """Generate documentation for the processor
        on the result backend"""
        # Generate Documentation
        index_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            self.metadata.version,
            self.index_filename,
        )
        index_create = generate_index_file(
            self.storage_backend, index_fpath, self.boundary["name"], self.metadata
        )
        self.provenance_log[
            f"{self.metadata.name} - created index documentation"
        ] = index_create
        license_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            self.metadata.version,
            self.license_filename,
        )
        license_create = generate_license_file(
            self.storage_backend, license_fpath, self.boundary["name"], self.metadata
        )
        self.provenance_log[
            f"{self.metadata.name} - created license documentation"
        ] = license_create
        self.log.debug("%s generated documentation on backend", self.metadata.name)

    def _fetch_source(self) -> str:
        """
        Fetch and unpack the required source data if required.
            Returns the path to existing files if they already exist

        return fpath str The path to the fetch source file
        """
        # Check if the files exist
        expected_source_path = os.path.join(
            self.processing_root_folder,
            "natural_earth_raster",
            "NE2_50M_SR",
            "NE2_50M_SR.tif",
        )
        if os.path.exists(expected_source_path):
            self.provenance_log[
                f"{self.metadata.name} - source files exist"
            ] = expected_source_path
            self.log.debug("Natural earth raster - source exists")
            return expected_source_path
        # Fetch the source zip
        self.log.debug("Natural earth raster - fetching zip")
        local_zip_fpath = self._fetch_zip()
        self.log.debug("Natural earth raster - fetched zip to %s", local_zip_fpath)
        self.provenance_log[
            f"{self.metadata.name} - zip download path"
        ] = local_zip_fpath
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
            f"{self.metadata.name} - valid GeoTIFF fetched/extracted"
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
            os.path.join(
                self.processing_root_folder, "natural_earth_raster", "NE2_50M_SR.zip"
            ),
        )
        return zip_path
