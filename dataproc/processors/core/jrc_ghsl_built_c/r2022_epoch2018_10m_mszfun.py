"""
JRC Built-C Processor
"""

import os
import inspect
import shutil
from typing import List

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
    generate_index_file,
    generate_license_file,
    generate_datapackage,
    output_filename
)
from dataproc.processors.core.jrc_ghsl_built_c.helpers import JRCBuiltCFetcher


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
    data_title = "GHS-BUILT-C MSZ and FC, R2022 E2018 10m"
    data_title_long = "JRC Global Human Settlement Layer - Built-Up Characteristics (GHS-BUILT-C - MSZ & FC) - Release 2022 - Epoch 2018 - 10m resolution - Morphological Settlement Zone & Functional Classification"
    data_summary = """
The spatial raster dataset delineates the boundaries of the human settlements at
10m resolution, and describe their inner characteristics in terms of the
morphology of the built environment and the functional use. The Morphological
Settlement Zone (MSZ) delineates the spatial domain of all the human settlements
at the neighboring scale of approx. 100m, based on the spatial generalization of
the built-up surface fraction (BUFRAC) function. The objective is to fill the
open spaces that are surrounded by large patches of built space. MSZ, open
spaces, and built spaces basic class abstractions are derived by mathematical
morphology spatial filtering (opening, closing, regional maxima) from the BUFRAC
function. They are further classified according to the information regarding
vegetation intensity (GHS-BUILT-C_VEG_GLOBE_R2022A), water surfaces
(GHS_LAND_GLOBE_R2022A), road surfaces (OSM highways), functional use
(GHS-BUILT-C_FUN_GLOBE_R2022A), and building height (GHS-BUILT-H_GLOBE_R2022A).

The main characteristics of this dataset are listed below. The complete
information about the GHSL main products can be found in the GHSL Data Package
2022 report (10.33 MB):
https://ghsl.jrc.ec.europa.eu/documents/GHSL_Data_Package_2022.pdf
    """
    data_citation = """
Dataset:

Pesaresi M., P. Panagiotis (2022): GHS-BUILT-C R2022A - GHS Settlement
Characteristics, derived from Sentinel2 composite (2018) and other GHS R2022A
data.European Commission, Joint Research Centre (JRC) PID:
http://data.europa.eu/89h/dde11594-2a66-4c1b-9a19-821382aed36e,
doi:10.2905/DDE11594-2A66-4C1B-9A19-821382AED36E

Concept & Methodology:

Schiavina M., Melchiorri M., Pesaresi M., Politis P., Freire S., Maffenini L.,
Florio P., Ehrlich D., Goch K., Tommasi P., Kemper T. GHSL Data Package 2022,
JRC 129516, ISBN 978-92-76-53071-8 doi:10.2760/19817 
    """
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://ghsl.jrc.ec.europa.eu/download.php?ds=builtC"
    data_formats = ["GeoTIFF"]


class Processor(BaseProcessorABC):
    """JRC GHSL Built C - R2022 - Epoch 2018 - 10m MSZ & Fun"""

    total_expected_files = 2
    source_fnames = [
        "GHS_BUILT_C_FUN_E2018_GLOBE_R2022A_54009_10_V1_0.tif",
        "GHS_BUILT_C_MSZ_E2018_GLOBE_R2022A_54009_10_V1_0.tif",
    ]
    index_filename = "index.html"
    license_filename = "license.html"

    def __init__(
        self,
        metadata: BaseMetadataABC,
        boundary: Boundary,
        storage_backend: StorageBackend,
        task_executor: task,
        processing_root_folder: str,
    ) -> None:
        super().__init__(
            metadata, boundary, storage_backend, task_executor, processing_root_folder
        )
        self.fetcher = JRCBuiltCFetcher()

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
        self.log.debug(
            "%s - collecting source geotiffs into %s",
            self.metadata.name,
            self.source_folder,
        )
        self.update_progress(10, "fetching and verifying source")
        source_fpaths = self._fetch_source()
        # Process MSZ and FUN
        self.log.debug("%s - cropping geotiffs", self.metadata.name)
        results_fpaths = []
        for idx, source_fpath in enumerate(source_fpaths):
            self.update_progress(
                20 + int(idx * (80 / len(source_fpaths))), "cropping source"
            )
            output_fpath = os.path.join(
                self.tmp_processing_folder, os.path.basename(source_fpath)
            )
            output_fpath = os.path.join(
                self.tmp_processing_folder, 
                output_filename(
                    self.metadata.name,
                    self.metadata.version,
                    self.boundary["name"],
                    'tif',
                    dataset_subfilename=os.path.splitext(os.path.basename(source_fpath))[0]
                )
            )
            # Crop Source - preserve Molleweide, assume we'll need BIGTIFF for this dataset
            crop_success = crop_raster(
                source_fpath,
                output_fpath,
                self.boundary,
                creation_options=["COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=6", "BIGTIFF=YES"],
            )
            self.log.debug(
                "%s %s - success: %s",
                self.metadata.name,
                os.path.basename(source_fpath),
                crop_success,
            )
            self.update_progress(
                20 + int(idx * (80 / len(source_fpaths))), "generating hash"
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
        ), f"{self.metadata.name} - number of successfully cropped files {len(results_fpaths)} do not match expected {self.total_expected_files}"

        self.update_progress(85, "moving result")
        # Move to Backend
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
        self.provenance_log[f"{self.metadata.name} - result URIs"] = ",".join(
            result_uris
        )

        self.log.debug("%s - generating documentation", self.metadata.name)

        # Generate documentation on backend
        self.update_progress(90, "generate documentation & datapackage")
        self.generate_documentation()

        self.log.debug("%s - generating datapackage meta", self.metadata.name)

        # Generate datapackage in log (using directory for URI)
        datapkg = generate_datapackage(
            self.metadata,
            result_uris,
            "GeoTIFF",
            [i["size"] for i in results_fpaths],
            [i["hash"] for i in results_fpaths],
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug(
            "%s generated datapackage in log: %s", self.metadata.name, datapkg
        )

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
                "%s - all source files appear to exist and are valid",
                self.metadata.name,
            )
            return [
                os.path.join(self.source_folder, source_fname)
                for source_fname in self.source_fnames
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
                    self.metadata.name,
                    fpath,
                    err,
                )
                if remove_invalid:
                    if os.path.exists(fpath):
                        os.remove(fpath)
                source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
