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
from dataproc.exceptions import FolderNotFoundException
from dataproc.helpers import (
    processor_name_from_file,
    version_name_from_file,
    crop_raster,
    assert_geotiff,
    data_file_hash,
    data_file_size,
    generate_index_file,
    generate_datapackage,
    generate_license_file,
    fetch_zenodo_doi,
    gp_crop_file_to_geopkg,
    assert_vector_file
)
from config import (
    LOCALFS_PROCESSING_BACKEND_ROOT,
)


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(inspect.stack()[1].filename)  # this must follow snakecase formatting, without special chars
    description = (
        "gridfinder - Predictive mapping of the global power system using open data"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = (
        "gridfinder"  # The dataset this processor targets
    )
    data_author = "Arderne, Christopher; NIcolas, Claire; Zorn, Conrad; Koks, Elco E"
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = (
        "https://doi.org/10.5281/zenodo.3628142"
    )


class Processor(BaseProcessorABC):
    """A Processor for Gridfinder"""

    zenodo_doi = "10.5281/zenodo.3628142"
    source_files = ['grid.gpkg', 'targets.tif', 'lv.tif']
    total_expected_files = len(source_files)
    index_filename = "index.html"
    license_filename = "license.html"

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
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

        self.log.debug("%s - cropping source", Metadata().name)
        results_fpaths = []
        for source_fpath in source_fpaths:
            output_fpath = os.path.join(
                self.tmp_processing_folder, os.path.basename(source_fpath)
            )
            if os.path.splitext(os.path.basename(source_fpath))[1] == '.tif':
                crop_success = crop_raster(source_fpath, output_fpath, self.boundary, preserve_raster_crs=True)
            elif os.path.splitext(os.path.basename(source_fpath))[1] == '.gpkg':
                crop_success = gp_crop_file_to_geopkg(
                    source_fpath,
                    self.boundary,
                    output_fpath,
                )
            else:
                continue
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
        datapkg = generate_datapackage(
            Metadata(),
            result_uris,
            "mixed",
            [i["size"] for i in results_fpaths],
            [i["hash"] for i in results_fpaths],
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug("%s generated datapackage in log: %s", Metadata().name, datapkg)

        return self.provenance_log

    def generate_documentation(self):
        """Generate documentation for the processor
        on the result backend"""
        # Generate Documentation
        index_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            Metadata().version,
            self.index_filename,
        )
        index_create = generate_index_file(
            self.storage_backend, index_fpath, self.boundary["name"], Metadata()
        )
        self.provenance_log[
            f"{Metadata().name} - created index documentation"
        ] = index_create
        license_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            Metadata().version,
            self.license_filename,
        )
        license_create = generate_license_file(
            self.storage_backend, license_fpath, self.boundary["name"], Metadata()
        )
        self.provenance_log[
            f"{Metadata().name} - created license documentation"
        ] = license_create
        self.log.debug("%s generated documentation on backend", Metadata().name)

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
            return [os.path.join(self.source_folder, _file) for _file in self.source_files]
        else:
            _ = fetch_zenodo_doi(self.zenodo_doi, self.source_folder)
            # Count the Tiffs
            self.log.debug("%s - Download Complete", Metadata().name)
            assert (
                self._all_source_exists()
            ), f"after {Metadata().name} download - not all source files were present"
            # Filter to just the files we support
            return [os.path.join(self.source_folder, _file) for _file in self.source_files]

    def _all_source_exists(self, remove_invalid=True) -> bool:
        """
        Check if all source files exist and are valid
            If not source will be removed
        """
        source_valid = [True for _ in range(len(self.source_files))]
        for idx, _file in enumerate(self.source_files):
            fpath = os.path.join(self.source_folder, _file)
            if os.path.splitext(_file)[1] == '.gpkg':
                try:
                    assert_vector_file(fpath)
                except Exception as err:
                    # remove the file and flag we should need to re-fetch, then move on
                    self.log.warning(
                        "%s source file %s appears to be invalid due to %s",
                        Metadata().name,
                        fpath,
                        err
                    )
                    if remove_invalid:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                    source_valid[idx] = False
            elif os.path.splitext(_file)[1] == '.tif':
                try:
                    assert_geotiff(fpath, check_compression=False, check_crs=None)
                except Exception as err:
                    # remove the file and flag we should need to re-fetch, then move on
                    self.log.warning(
                        "%s source file %s appears to be invalid due to %s",
                        Metadata().name,
                        fpath,
                        err
                    )
                    if remove_invalid:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                    source_valid[idx] = False
            else:
                continue
        return all(source_valid)
