"""
JRC Population Processor
"""

import os
import inspect
import shutil

from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)
from dataproc import DataPackageLicense
from dataproc.helpers import (
    processor_name_from_file,
    version_name_from_file,
    crop_raster,
    assert_geotiff,
    data_file_hash,
    data_file_size,
    generate_datapackage,
    generate_index_file,
    generate_license_file,
)
from dataproc.exceptions import FolderNotFoundException
from dataproc.processors.core.jrc_ghsl_population.helpers import JRCPopFetcher


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(
        inspect.stack()[1].filename
    )  # this must follow snakecase formatting, without special chars
    description = "A Processor for JRC GHSL Population - R2022 release, Epoch 2020, 1Km resolution"  # Longer processor description
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "r2022_epoch2020_1km"  # The dataset this processor targets
    data_author = "Joint Research Centre"
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://ghsl.jrc.ec.europa.eu/download.php?ds=pop"


class Processor(BaseProcessorABC):
    """JRC GHSL Population - R2020 - Epoch 2020 - 1Km"""

    total_expected_files = 1
    source_fnames = ["GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0.tif"]
    zip_url = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2022A/GHS_POP_E2020_GLOBE_R2022A_54009_1000/V1-0/GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0.zip"
    index_filename = "index.html"
    license_filename = "license.html"

    def exists(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        try:
            count_on_backend = self.storage_backend.count_boundary_data_files(
                self.boundary["name"],
                self.metadata.name,
                self.metadata.version,
                datafile_ext=".tif",
            )
        except FolderNotFoundException:
            return False
        return count_on_backend == self.total_expected_files

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            self.provenance_log[self.metadata.name] = "exists"
            return self.provenance_log
        else:
            # Ensure we start with a blank output folder on the storage backend
            try:
                self.storage_backend.remove_boundary_data_files(
                    self.boundary["name"],
                    self.metadata.name,
                    self.metadata.version,
                )
            except FolderNotFoundException:
                pass
            # Cleanup anything in tmp processing
            self._clean_tmp_processing()
        # Check if the source TIFF exists and fetch it if not
        self.log.debug(
            "%s - collecting source geotiffs into %s",
            self.metadata.name,
            self.source_folder,
        )
        self.update_progress(10, "fetching source")
        source_fpath = self._fetch_source()
        output_fpath = os.path.join(
            self.tmp_processing_folder, os.path.basename(source_fpath)
        )
        # Crop Source - preserve Molleweide
        self.update_progress(50, "cropping source")
        self.log.debug("%s - cropping source", self.metadata.name)
        crop_success = crop_raster(
            source_fpath, output_fpath, self.boundary, preserve_raster_crs=True
        )
        self.log.debug(
            "%s %s - success: %s",
            self.metadata.name,
            os.path.basename(source_fpath),
            crop_success,
        )
        self.update_progress(70, "moving result")
        self.log.debug("%s - moving cropped data to backend", self.metadata.name)
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
        )
        self.provenance_log[f"{self.metadata.name} - move to storage success"] = (
            result_uri is not None
        )
        self.provenance_log[f"{self.metadata.name} - result URI"] = result_uri

        self.update_progress(80, "generate documentation & datapackage")
        # Generate documentation on backend
        self.log.debug("%s - generating documentation", self.metadata.name)
        self.generate_documentation()

        # Generate datapackage in log (using directory for URI)
        self.log.debug("%s - generating datapackage meta", self.metadata.name)
        output_hash = data_file_hash(output_fpath)
        output_size = data_file_size(output_fpath)
        datapkg = generate_datapackage(
            self.metadata, [result_uri], "GeoTIFF", [output_size], [output_hash]
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug("%s generated datapackage in log: %s", self.metadata.name, datapkg)

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

    def _clean_tmp_processing(self):
        """Remove the tmp processing folder and recreate"""
        # Remove partial previous tmp results if they exist
        if os.path.exists(self.tmp_processing_folder):
            shutil.rmtree(self.tmp_processing_folder)
        # Generate the tmp output directory
        os.makedirs(self.tmp_processing_folder, exist_ok=True)

    def _fetch_source(self) -> str:
        """
        Fetch and unpack the required source data if required.
            Returns the path to existing files if they already exist

        return fpath str The path to the fetch source file
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid", self.metadata.name
            )
            return os.path.join(self.source_folder, self.source_fnames[0])
        else:
            fetcher = JRCPopFetcher()
            source_geotif_fpath = fetcher.fetch_source(self.zip_url, self.source_folder)
            return source_geotif_fpath

    def _all_source_exists(self, remove_invalid=True) -> bool:
        """
        Check if all source files exist and are valid
            If not source will be removed
        """
        source_valid = True
        count_tiffs = 0
        for source_fname in self.source_fnames:
            fpath = os.path.join(self.source_folder, source_fname)
            try:
                assert_geotiff(fpath, check_compression=False, check_crs="ESRI:54009")
                count_tiffs += 1
            except Exception as err:
                # remove the file and flag we should need to re-fetch, then move on
                self.log.warning(
                    "%s source file appears to be invalid or missing - removing %s due to %s",
                    self.metadata.name,
                    fpath,
                    err,
                )
                if remove_invalid:
                    if os.path.exists(fpath):
                        os.remove(fpath)
                source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
