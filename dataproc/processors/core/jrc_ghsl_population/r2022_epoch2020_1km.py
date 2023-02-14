"""
JRC Population Processor
"""

import os
import logging
import inspect
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
    generate_datapackage,
    generate_index_file,
    generate_license_file
)
from dataproc.exceptions import FolderNotFoundException
from dataproc.processors.core.jrc_ghsl_population.helpers import JRCPopFetcher
from config import LOCALFS_PROCESSING_BACKEND_ROOT


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(inspect.stack()[1].filename)  # this must follow snakecase formatting, without special chars
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
    source_fnames = [
        'GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0.tif'
    ]
    zip_url = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2022A/GHS_POP_E2020_GLOBE_R2022A_54009_1000/V1-0/GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0.zip"
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
        self.log.debug("%s - collecting source geotiffs into %s", Metadata().name, self.source_folder)
        source_fpath = self._fetch_source()
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
        self.log.debug("%s - generating datapackage meta", Metadata().name)

        self.log.debug("%s - moving cropped data to backend", Metadata().name)
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary["name"],
            Metadata().name,
            Metadata().version,
        )

        self.provenance_log[f"{Metadata().name} - move to storage success"] = (
            result_uri is not None
        )
        self.provenance_log[f"{Metadata().name} - result URI"] = result_uri

        # Generate documentation on backend
        self.generate_documentation()

        # Generate datapackage in log (using directory for URI)
        output_hash = data_file_hash(output_fpath)
        output_size = data_file_size(output_fpath)
        datapkg = generate_datapackage(
            Metadata(), [result_uri], "GeoTIFF", [output_size], [output_hash]
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug("%s generated datapackage in log: %s", Metadata().name, datapkg)

        return self.provenance_log

    def generate_documentation(self):
        """Generate documentation for the processor
        on the result backend"""
        # Generate Documentation
        index_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "templates", Metadata().version, self.index_filename
        )
        index_create = generate_index_file(
            self.storage_backend,
            index_fpath, 
            self.boundary["name"],
            Metadata()
        )
        self.provenance_log[f"{Metadata().name} - created index documentation"] = index_create
        license_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "templates", Metadata().version, self.license_filename
        )
        license_create = generate_license_file(
            self.storage_backend,
            license_fpath, 
            self.boundary["name"],
            Metadata()
        )
        self.provenance_log[f"{Metadata().name} - created license documentation"] = license_create
        self.log.debug("%s generated documentation on backend", Metadata().name)

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
                "%s - all source files appear to exist and are valid", Metadata().name
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
                    Metadata().name,
                    fpath,
                    err
                )
                if remove_invalid:
                    if os.path.exists(fpath):
                        os.remove(fpath)
                source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
