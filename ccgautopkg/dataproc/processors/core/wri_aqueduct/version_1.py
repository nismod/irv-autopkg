"""
WRI Aqueduct Processor
"""

import os
import logging
import inspect
import shutil

from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary
from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
    DataPackageLicense,
)
from dataproc.helpers import version_name_from_file, crop_raster, assert_geotiff
from dataproc.processors.core.wri_aqueduct.helpers import HazardAqueduct
from dataproc.exceptions import FolderNotFoundException
from config import LOCALFS_PROCESSING_BACKEND_ROOT


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = (
        "wri_aqueduct"  # this must follow snakecase formatting, without special chars
    )
    description = "A Processor for WRI Aqueduct"  # Longer processor description
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "wri_aqueduct"  # The dataset this processor targets
    data_author = "WRI"
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = (
        "http://wri-projects.s3.amazonaws.com/AqueductFloodTool/download/v2/index.html"
    )


class Processor(BaseProcessorABC):
    """A Processor for WRI Aqueduct"""

    total_expected_files = 379
    index_filename = "index.html"
    license_filename = "license.html"

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
        # Custom init vars for this processor
        self.aqueduct_fetcher = HazardAqueduct()

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
        try:
            count_on_backend = self.storage_backend.count_boundary_data_files(
                self.boundary["name"],
                Metadata().name,
                Metadata().version,
                datafile_ext=".tif",
            )
        except FolderNotFoundException:
            return False
        return count_on_backend == self.total_expected_files

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            self.provenance_log[Metadata().name] = "exists"
            return self.provenance_log
        else:
            # Ensure we start with a blank output folder on the storage backend
            try:
                self.storage_backend.remove_boundary_data_files(
                    self.boundary["name"],
                    Metadata().name,
                    Metadata().version,
                )
            except FolderNotFoundException:
                pass
        # Check if the source TIFF exists and fetch it if not
        self._fetch_source()

        # Remove partial previous tmp results if they exist
        if os.path.exists(self.tmp_processing_folder):
            shutil.rmtree(self.tmp_processing_folder)
        # Generate the tmp output directory
        os.makedirs(self.tmp_processing_folder, exist_ok=True)

        self.log.debug("WRI Aqueduct - cropping geotiffs")
        results_fpaths = []
        for fileinfo in os.scandir(self.source_folder):
            if not os.path.splitext(fileinfo.name)[1] == ".tif":
                self.log.debug(
                    "aqueduct skipped non-tif in source dir: %s", fileinfo.name
                )
                continue
            geotiff_fpath = os.path.join(self.source_folder, fileinfo.name)
            output_fpath = os.path.join(self.tmp_processing_folder, fileinfo.name)
            assert_geotiff(geotiff_fpath)
            crop_success = crop_raster(geotiff_fpath, output_fpath, self.boundary)
            self.log.debug(
                "aqueduct crop %s - success: %s", fileinfo.name, crop_success
            )
            if crop_success:
                results_fpaths.append(output_fpath)
        # Check results look sensible
        assert (
            len(results_fpaths) == self.total_expected_files
        ), f"number of successfully cropped files {len(results_fpaths)} do not match expected {self.total_expected_files}"

        self.log.debug("WRI Aqueduct - moving cropped data to backend")
        result_uris = []
        for fpath in results_fpaths:
            result_uri = self.storage_backend.put_processor_data(
                fpath,
                self.boundary["name"],
                Metadata().name,
                Metadata().version,
            )
            result_uris.append(result_uri)

        self.provenance_log[f"{Metadata().name} - move to storage success"] = (
            len(result_uris) == self.total_expected_files
        )

        self.provenance_log[f"{Metadata().name} - result URIs"] = ",".join(result_uris)

        # Generate Documentation
        index_fpath = self._generate_index_file()
        index_create = self.storage_backend.put_boundary_data(
            index_fpath, self.boundary["name"]
        )
        self.provenance_log[
            f"{Metadata().name} - created index documentation"
        ] = index_create
        license_fpath = self._generate_license_file()
        license_create = self.storage_backend.put_boundary_data(
            license_fpath, self.boundary["name"]
        )
        self.provenance_log[
            f"{Metadata().name} - created license documentation"
        ] = license_create

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

    def _fetch_source(self):
        """
        Fetch and unpack the required source data if required.
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "WRI Aqueduct - all source files appear to exist and are valid"
            )
            return
        else:
            self.aqueduct_fetcher = HazardAqueduct()
            metadata = self.aqueduct_fetcher.file_metadata()
            if self.total_expected_files != len(metadata):
                self.log.warning(
                    "Aqueduct limiting total_expected_files to %s",
                    self.total_expected_files,
                )
                metadata = metadata[: self.total_expected_files]
            # Generate path info by downloading (will skip if files exist - invalid files have been removed in the source check above)
            metadata = self.aqueduct_fetcher.download_files(
                metadata,
                self.source_folder,
            )
            # Count the Tiffs
            self.log.debug("WRI Aqueduct - Download Complete")
            assert (
                len(metadata) == self.total_expected_files
            ), "after aqueduct download - not all source files were present"
            return

    def _all_source_exists(self, remove_invalid=True) -> bool:
        """
        Check if all source files exist and are valid
            If not source will be removed
        """
        source_valid = True
        count_tiffs = 0
        for fileinfo in os.scandir(self.source_folder):
            if os.path.splitext(fileinfo.name)[1] == ".tif":
                fpath = os.path.join(self.source_folder, fileinfo.name)
                try:
                    assert_geotiff(fpath)
                    count_tiffs += 1
                except Exception:
                    # remove the file and flag we should need to re-fetch, then move on
                    self.log.warning(
                        "Aqueduct source file appears to be invalid - removing"
                    )
                    if remove_invalid:
                        os.remove(fpath)
                    source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
