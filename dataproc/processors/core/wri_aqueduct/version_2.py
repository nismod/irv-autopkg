"""
WRI Aqueduct Processor
"""

import os
import inspect
import shutil

from celery.app import task

from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)
from dataproc.backends import StorageBackend
from dataproc import Boundary, DataPackageLicense
from dataproc.helpers import (
    processor_name_from_file,
    version_name_from_file,
    crop_raster,
    assert_geotiff,
    data_file_hash,
    data_file_size,
    generate_license_file,
    generate_datapackage,
    generate_index_file,
    output_filename
)
from dataproc.processors.core.wri_aqueduct.helpers import HazardAqueduct


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(
        inspect.stack()[1].filename
    )  # this must follow snakecase formatting, without special chars
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

    def __init__(self, metadata: BaseMetadataABC, boundary: Boundary, storage_backend: StorageBackend, task_executor: task, processing_root_folder: str) -> None:
        super().__init__(metadata, boundary, storage_backend, task_executor, processing_root_folder)
        self.aqueduct_fetcher = HazardAqueduct()

    def exists(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        try:
            count_on_backend = self.storage_backend.count_boundary_data_files(
                self.boundary["name"],
                self.metadata.name,
                self.metadata.version,
                datafile_ext=".tif",
            )
        except FileNotFoundError:
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
            except FileNotFoundError:
                pass
        # Check if the source TIFF exists and fetch it if not
        self.update_progress(10, "fetching and verifying source")
        self._fetch_source()

        self.log.debug("WRI Aqueduct - cropping geotiffs")
        results_fpaths = []
        for idx, fileinfo in enumerate(os.scandir(self.source_folder)):
            if not os.path.splitext(fileinfo.name)[1] == ".tif":
                self.log.warning(
                    "aqueduct skipped non-tif in source dir: %s", fileinfo.name
                )
                continue
            self.update_progress(
                10 + int(idx * (80 / self.total_expected_files)), "cropping source"
            )
            geotiff_fpath = os.path.join(self.source_folder, fileinfo.name)
            
            subfilename = os.path.splitext(fileinfo.name)[0]
            output_fpath = os.path.join(
                self.tmp_processing_folder, 
                output_filename(
                    self.metadata.name,
                    self.metadata.version,
                    self.boundary["name"],
                    'tif',
                    dataset_subfilename=subfilename
                )
            )

            assert_geotiff(geotiff_fpath)
            crop_success = crop_raster(geotiff_fpath, output_fpath, self.boundary)
            self.log.debug(
                "%s crop %s - success: %s", self.metadata.name, fileinfo.name, crop_success
            )
            if crop_success:
                results_fpaths.append(
                    {
                        "fpath": output_fpath,
                        "hash": data_file_hash(output_fpath),
                        "size": data_file_size(output_fpath),
                    }
                )
        # Check results look sensible
        assert (
            len(results_fpaths) == self.total_expected_files
        ), f"number of successfully cropped files {len(results_fpaths)} do not match expected {self.total_expected_files}"

        self.log.debug("%s - moving cropped data to backend", self.metadata.name)
        self.update_progress(85, "moving result")
        result_uris = []
        for result in results_fpaths:
            result_uri = self.storage_backend.put_processor_data(
                result["fpath"],
                self.boundary["name"],
                self.metadata.name,
                self.metadata.version,
            )
            result_uris.append(result_uri)

        self.provenance_log[f"{self.metadata.name} - move to storage success"] = (
            len(result_uris) == self.total_expected_files
        )
        self.provenance_log[f"{self.metadata.name} - result URIs"] = ",".join(result_uris)

        # Generate documentation on backend
        self.update_progress(90, "generate documentation & datapackage")
        self.generate_documentation()

        # Generate datapackage in log (using directory for URI)
        datapkg = generate_datapackage(
            self.metadata,
            result_uris,
            "GeoTIFF",
            [i["size"] for i in results_fpaths],
            [i["hash"] for i in results_fpaths],
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

    def _fetch_source(self):
        """
        Fetch and unpack the required source data if required.
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid", self.metadata.name
            )
            return
        else:
            self.aqueduct_fetcher = HazardAqueduct()
            metadata = self.aqueduct_fetcher.file_metadata()
            if self.total_expected_files != len(metadata):
                self.log.warning(
                    "%s limiting total_expected_files to %s",
                    self.metadata.name,
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
                    assert_geotiff(fpath, check_compression=False)
                    count_tiffs += 1
                except Exception:
                    # remove the file and flag we should need to re-fetch, then move on
                    self.log.warning(
                        "Aqueduct source file appears to be invalid - removing"
                    )
                    if remove_invalid is True:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                    source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
