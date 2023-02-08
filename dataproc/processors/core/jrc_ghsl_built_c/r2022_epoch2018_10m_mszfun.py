"""
JRC Built-C Processor
"""

import os
import logging
import inspect
import shutil
from typing import List

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
)
from dataproc.exceptions import FolderNotFoundException
from dataproc.processors.core.jrc_ghsl_built_c.helpers import JRCBuiltCFetcher
from config import LOCALFS_PROCESSING_BACKEND_ROOT


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(
        inspect.stack()[1].filename
    )  # this must follow snakecase formatting, without special chars
    description = """
        A Processor for JRC GHSL Built-Up Characteristics - 
        R2022 release, Epoch 2018, 10m resolution, Morphological Settlement Zone and Functional classification
    """  # Longer processor description
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "r2022_epoch2018_10m_mszfun"  # The dataset this processor targets
    data_author = "Joint Research Centre"
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://ghsl.jrc.ec.europa.eu/download.php?ds=builtC"


class Processor(BaseProcessorABC):
    """JRC GHSL Built C - R2022 - Epoch 2018 - 10m MSZ & Fun"""

    total_expected_files = 2
    source_fnames = [
        'GHS_BUILT_C_FUN_E2018_GLOBE_R2022A_54009_10_V1_0.tif',
        'GHS_BUILT_C_MSZ_E2018_GLOBE_R2022A_54009_10_V1_0.tif'
    ]
    index_filename = "index.html"
    license_filename = "license.html"

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
        # Init the Datafile fetcher
        self.fetcher = JRCBuiltCFetcher()

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
            # Cleanup anything in tmp processing
            self._clean_tmp_processing()
        # Check if the source TIFF exists and fetch it if not
        self.log.debug(
            "%s - collecting source geotiffs into %s",
            Metadata().name,
            self.source_folder,
        )
        source_fpaths = self._fetch_source()
        # Process MSZ and FUN
        self.log.debug("%s - cropping geotiffs", Metadata().name)
        results_fpaths = []
        for source_fpath in source_fpaths:
            output_fpath = os.path.join(
                self.tmp_processing_folder, os.path.basename(source_fpath)
            )
            # Crop Source - preserve Molleweide
            crop_success = crop_raster(
                source_fpath, output_fpath, self.boundary, preserve_raster_crs=True
            )
            self.log.debug(
                "%s %s - success: %s",
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

        # Move to Backend
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

        self.log.debug("%s - generating documentation", Metadata().name)

        # Generate documentation on backend
        self.generate_documentation()

        self.log.debug("%s - generating datapackage meta", Metadata().name)

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
        self.provenance_log["datapackage"] = datapkg
        self.log.debug("%s generated datapackage in log: %s", Metadata().name, datapkg)

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
        self.log.debug("%s generated documentation on backend", Metadata().name)

    def _generate_index_file(self) -> str:
        """
        Generate the index documentation file

        ::returns dest_fpath str Destination filepath on the processing backend
        """
        template_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            Metadata().version,
            self.index_filename,
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
            Metadata().version,
            self.license_filename,
        )
        return template_fpath

    def _clean_tmp_processing(self):
        """Remove the tmp processing folder and recreate"""
        # Remove partial previous tmp results if they exist
        if os.path.exists(self.tmp_processing_folder):
            shutil.rmtree(self.tmp_processing_folder)
        # Generate the tmp output directory
        os.makedirs(self.tmp_processing_folder, exist_ok=True)

    def _fetch_source(self) -> List[str]:
        """
        Fetch and unpack the required source data if required.
            Returns the path to existing files if they already exist

        return fpath str The path to the fetch source file
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid", Metadata().name
            )
            return [
                os.path.join(self.source_folder, source_fname) for source_fname in self.source_fnames
            ]
        else:
            source_geotif_fpaths = self.fetcher.fetch_source(self.source_folder)
            return source_geotif_fpaths

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
                    "%s source file appears to be invalid - removing %s due to %s",
                    Metadata().name,
                    fpath,
                    err
                )
                if remove_invalid:
                    os.remove(fpath)
                source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
