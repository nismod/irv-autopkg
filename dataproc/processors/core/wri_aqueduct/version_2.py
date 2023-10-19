"""
WRI Aqueduct Processor
"""

import os

from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)
from dataproc.storage import StorageBackend
from dataproc import Boundary, DataPackageLicense
from dataproc.helpers import (
    crop_raster,
    assert_geotiff,
    datapackage_resource,
)
from dataproc.processors.core.wri_aqueduct.helpers import HazardAqueduct


class Metadata(BaseMetadataABC):
    description = "A Processor for WRI Aqueduct"  # Longer processor description
    dataset_name = "wri_aqueduct"  # The dataset this processor targets
    data_title = "Aqueduct Flood Hazard Maps"
    data_title_long = "World Resource Institute - Aqueduct Flood Hazard Maps (Version 2, updated October 20, 2020)"
    data_author = "Ward, P.J., H.C. Winsemius, S. Kuzma, M.F.P. Bierkens, A. Bouwman, H. de Moel, A. Díaz Loaiza, et al."
    data_summary = """World Resource Institute - Aqueduct Flood Hazard Maps (Version 2 (updated
October 20, 2020)).  Inundation depth in meters for coastal and riverine
floods over 1km grid squares. 1 in 2 to 1 in 1000 year return periods.
Baseline, RCP 4.5 & 8.5 emission scenarios. Current and future maps in 2030,
2050 and 2080."""
    data_citation = """
Ward, P.J., H.C. Winsemius, S. Kuzma, M.F.P. Bierkens, A. Bouwman, H. de Moel, A. Díaz Loaiza, et al. 2020.
Aqueduct Floods Methodology. Technical Note. Washington, D.C.: World Resources Institute. Available online at:
www.wri.org/publication/aqueduct-floods-methodology."""
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = (
        "http://wri-projects.s3.amazonaws.com/AqueductFloodTool/download/v2/index.html"
    )
    data_formats = ["GeoTIFF"]


class Processor(BaseProcessorABC):
    """A Processor for WRI Aqueduct"""

    total_expected_files = 379

    def __init__(
        self,
        metadata: BaseMetadataABC,
        boundary: Boundary,
        storage_backend: StorageBackend,
        task_executor,
        processing_root_folder: str,
    ) -> None:
        super().__init__(
            metadata, boundary, storage_backend, task_executor, processing_root_folder
        )
        self.aqueduct_fetcher = HazardAqueduct()

    def generate(self):
        """Generate files for a given processor"""

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
                self.output_filename(
                    self.metadata.name,
                    self.metadata.version,
                    self.boundary["name"],
                    "tif",
                    dataset_subfilename=subfilename,
                ),
            )

            assert_geotiff(geotiff_fpath)
            crop_success = crop_raster(geotiff_fpath, output_fpath, self.boundary)
            self.log.debug(
                "%s crop %s - success: %s",
                self.metadata.name,
                fileinfo.name,
                crop_success,
            )
            if crop_success:
                results_fpaths.append(output_fpath)
        # Check results look sensible
        assert (
            len(results_fpaths) == self.total_expected_files
        ), f"number of successfully cropped files {len(results_fpaths)} do not match expected {self.total_expected_files}"

        self.log.debug("%s - moving cropped data to backend", self.metadata.name)
        self.update_progress(85, "moving result")
        result_uris = []
        for fpath in results_fpaths:
            result_uri = self.storage_backend.put_processor_data(
                fpath,
                self.boundary["name"],
                self.metadata.name,
                self.metadata.version,
            )
            result_uris.append(result_uri)

        self.provenance_log[f"{self.metadata.name} - move to storage success"] = (
            len(result_uris) == self.total_expected_files
        )
        self.provenance_log[f"{self.metadata.name} - result URIs"] = ",".join(
            result_uris
        )

        # Generate documentation on backend
        self.update_progress(90, "generate documentation & datapackage")
        self.generate_documentation()

        hashes, sizes = self.calculate_files_metadata(results_fpaths)
        # Generate datapackage in log (using directory for URI)
        datapkg = datapackage_resource(
            self.metadata,
            result_uris,
            "GeoTIFF",
            sizes,
            hashes,
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug(
            "%s generated datapackage in log: %s", self.metadata.name, datapkg
        )

        return self.provenance_log

    def _fetch_source(self):
        """
        Fetch and unpack the required source data if required.
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid",
                self.metadata.name,
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
