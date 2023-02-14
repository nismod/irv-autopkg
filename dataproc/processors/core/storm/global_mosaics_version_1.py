"""
STORM (Global Mosaics Version 1) Processor
"""

import os
import logging
import inspect
from typing import List
import shutil

from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)
from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary, DataPackageLicense
from dataproc.helpers import (
    processor_name_from_file,
    version_name_from_file,
    crop_raster,
    assert_geotiff,
    data_file_hash,
    data_file_size,
    datapackage_resource,
    fetch_zenodo_doi,
    tiffs_in_folder,
)
from dataproc.exceptions import FolderNotFoundException
from config import LOCALFS_PROCESSING_BACKEND_ROOT


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
    dataset_name = "STORM Global Mosaics 10.5281/zenodo.7438145"  # The dataset this processor targets
    data_author = "University of Oxford"
    data_license = DataPackageLicense(
        name="CC0",
        title="CC0",
        path="https://creativecommons.org/share-your-work/public-domain/cc0/",
    )
    data_origin_url = "https://zenodo.org/record/7438145#.Y-S6cS-l30o"


class Processor(BaseProcessorABC):
    """A Processor for STORM Global Mosaics"""

    zenodo_doi = "10.5281/zenodo.7438145"
    total_expected_files = 140
    index_filename = f"{Metadata().version}/index.html"
    license_filename = f"{Metadata().version}/license.html"

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        """"""
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.paths_helper = PathsHelper(
            os.path.join(
                LOCALFS_PROCESSING_BACKEND_ROOT, Metadata().name, Metadata().version
            )
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
        source_fpaths = self._fetch_source()

        self.log.debug("%s - cropping geotiffs", Metadata().name)
        results_fpaths = []
        for source_fpath in source_fpaths:
            output_fpath = os.path.join(
                self.tmp_processing_folder, os.path.basename(source_fpath)
            )
            crop_success = crop_raster(source_fpath, output_fpath, self.boundary)
            self.log.debug(
                "%s crop %s - success: %s",
                Metadata().name,
                os.path.basename(source_fpath),
                crop_success,
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
        ), f"{Metadata().name} - number of successfully cropped files {len(results_fpaths)} do not match expected {self.total_expected_files}"

        self.log.debug("%s - moving cropped data to backend", Metadata().name)
        result_uris = []
        for result in results_fpaths:
            result_uri = self.storage_backend.put_processor_data(
                result["fpath"],
                self.boundary["name"],
                Metadata().name,
                Metadata().version,
            )
            result_uris.append(result_uri)
        self.provenance_log[f"{Metadata().name} - move to storage success"] = (
            len(result_uris) == self.total_expected_files
        )
        self.provenance_log[f"{Metadata().name} - result URIs"] = ",".join(result_uris)

        # Generate documentation on backend
        self.generate_documentation()

        # Generate datapackage in log (using directory for URI)
        self.generate_datapackage(result_uris, results_fpaths)

        return self.provenance_log

    def generate_datapackage(self, uris: str, results: List[dict]):
        """Generate the datapackage resource for this processor
        and append to processor log
        """
        # Generate the datapackage and add it to the output log
        datapkg = datapackage_resource(
            Metadata(),
            uris,
            "GeoTIFF",
            [i["size"] for i in results],
            [i["hash"] for i in results],
        )
        self.provenance_log["datapackage"] = datapkg.asdict()
        self.log.debug("%s generated datapackage in log: %s", Metadata().name, datapkg.asdict())

    def generate_documentation(self):
        """Generate documentation for the processor
        on the result backend"""
        index_fpath = self._generate_index_file()
        index_create = self.storage_backend.put_processor_metadata(
            index_fpath,
            self.boundary["name"],
            Metadata().name,
            Metadata().version,
        )
        self.provenance_log[
            f"{Metadata().name} - created index documentation"
        ] = index_create
        license_fpath = self._generate_license_file()
        license_create = self.storage_backend.put_processor_metadata(
            license_fpath,
            self.boundary["name"],
            Metadata().name,
            Metadata().version,
        )
        self.provenance_log[
            f"{Metadata().name} - created license documentation"
        ] = license_create
        self.log.debug("Aqueduct generated documentation on backend")

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

    def _fetch_source(self) -> List[str]:
        """
        Fetch and unpack the required source data if required.

        ::returns source_fpaths List[str] Filepaths of all source data
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid", Metadata().name
            )
            return tiffs_in_folder(self.source_folder, full_paths=True)
        else:
            source_fpaths = fetch_zenodo_doi(self.zenodo_doi, self.source_folder)
            # Count the Tiffs
            self.log.debug("%s - Download Complete", Metadata().name)
            assert (
                len(source_fpaths) == self.total_expected_files
            ), f"after {Metadata().name} download - not all source files were present"
            return source_fpaths

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
                        "%s source file appears to be invalid - removing",
                        Metadata().name,
                    )
                    if remove_invalid:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                    source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
