"""
ISIMP Drought V1 Processor
"""

import os
import shutil
from typing import List

from dataproc import DataPackageLicense
from dataproc.exceptions import ProcessorDatasetExists
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    tiffs_in_folder,
    crop_raster,
    assert_geotiff,
    datapackage_resource,
    fetch_zenodo_doi,
    output_filename,
    unpack_zip,
)
from .helpers import VERSION_1_SOURCE_FILES


class Metadata(BaseMetadataABC):
    description = "ISIMP Drought v1 processor"  # Longer processor description
    dataset_name = "ISIMP Drought"  # The dataset this processor targets
    data_author = "Lange, S., Volkholz, J., Geiger, T., Zhao, F., Vega, I., Veldkamp, T., et al. (2020)"
    data_title = "ISIMP Drought"
    data_title_long = "Annual probability of extreme heat and drought events, derived from Lange et al 2020"
    data_summary = """
The time series of extreme events given by Lange et al has been processed into an annual probability of occurrence by researchers at the University of Oxford, using the pipeline available online at https://github.com/nismod/infra-risk-vis/blob/45d8974c311067141ee6fcaa1321c7ecdaa59752/etl/pipelines/isimip/Snakefile - this is a draft dataset, used for visualisation in https://global.infrastructureresilience.org/ but not otherwise reviewed or published.

If you use this, please cite: Lange, S., Volkholz, J., Geiger, T., Zhao, F., Vega, I., Veldkamp, T., et al. (2020). Projecting exposure to extreme climate impact events across six event categories and three spatial scales. Earth's Future, 8, e2020EF001616. DOI 10.1029/2020EF001616

This is shared under a CC0 1.0 Universal Public Domain Dedication (CC0 1.0) When using ISIMIP data for your research, please appropriately credit the data providers, e.g. either by citing the DOI for the dataset, or by appropriate acknowledgment.

Annual probability of drought (soil moisture below a baseline threshold) or extreme heat (temperature and humidity-based indicators over a threshold) events on a 0.5° grid. 8 hydrological models forced by 4 GCMs under baseline, RCP 2.6 & 6.0 emission scenarios. Current and future maps in 2030, 2050 and 2080.

The ISIMIP2b climate input data and impact model output data analyzed in this study are available in the ISIMIP data repository at ESGF, see https://esg.pik-potsdam.de/search/isimip/?project=ISIMIP2b&product=input and https://esg.pik-potsdam.de/search/isimip/?project=ISIMIP2b&product=output, respectively. More information about the GHM, GGCM, and GVM output data is provided by Gosling et al. (2020), Arneth et al. (2020), and Reyer et al. (2019), respectively.

Event definitions are given in Lange et al, table 1. Land area is exposed to drought if monthly soil moisture falls below the 2.5th percentile of the preindustrial baseline distribution for at least seven consecutive months. Land area is exposed to extreme heat if both a relative indicator based on temperature (Russo et al 2015, 2017) and an absolute indicator based on temperature and relative humidity (Masterton & Richardson, 1979) exceed their respective threshold value.
    """
    data_citation = """
Lange, S., Volkholz, J., Geiger, T., Zhao, F., Vega, I., Veldkamp, T., et al. (2020). Projecting exposure to extreme climate impact events across six event categories and three spatial scales. Earth's Future, 8, e2020EF001616. DOI 10.1029/2020EF001616
    """
    data_license = DataPackageLicense(
        name="CC0",
        title="CC0",
        path="https://creativecommons.org/share-your-work/public-domain/cc0/",
    )
    data_origin_url = "https://doi.org/10.5281/zenodo.7732393"
    data_formats = ["GeoTIFF"]


class Processor(BaseProcessorABC):
    """A Processor for ISIMP Drought V1"""

    zenodo_doi = "10.5281/zenodo.7732393"
    source_files = VERSION_1_SOURCE_FILES
    total_expected_files = len(source_files)
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
        except FileNotFoundError:
            return False
        return count_on_backend == self.total_expected_files

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            raise ProcessorDatasetExists()
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
        source_fpaths = self._fetch_source()

        self.log.debug("%s - cropping source", self.metadata.name)
        results_fpaths = []
        for idx, source_fpath in enumerate(source_fpaths):
            self.update_progress(
                10 + int(idx * (80 / len(source_fpaths))), "cropping source"
            )

            subfilename = os.path.splitext(os.path.basename(source_fpath))[0]
            file_format = os.path.splitext(os.path.basename(source_fpath))[1]

            output_fpath = os.path.join(
                self.tmp_processing_folder,
                output_filename(
                    self.metadata.name,
                    self.metadata.version,
                    self.boundary["name"],
                    file_format,
                    dataset_subfilename=subfilename,
                ),
            )
            crop_success = crop_raster(source_fpath, output_fpath, self.boundary)

            self.log.debug(
                "%s crop %s - success: %s",
                self.metadata.name,
                os.path.basename(source_fpath),
                crop_success,
            )
            if crop_success:
                results_fpaths.append(output_fpath)
        # Check results look sensible
        assert (
            len(results_fpaths) == self.total_expected_files
        ), f"{self.metadata.name} - number of successfully cropped files {len(results_fpaths)} do not match expected {self.total_expected_files}"

        self.update_progress(85, "moving result")
        self.log.debug("%s - moving cropped data to backend", self.metadata.name)
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

        # Generate datapackage in log (using directory for URI)
        hashes, sizes = self.calculate_files_metadata(results_fpaths)
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

    def _fetch_source(self) -> List[str]:
        """
        Fetch and unpack the required source data if required.

        ::returns source_fpaths List[str] Filepaths of all source data
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid",
                self.metadata.name,
            )
            return [
                os.path.join(self.source_folder, _file) for _file in self.source_files
            ]
        else:
            downloaded_files = fetch_zenodo_doi(
                self.zenodo_doi, self.source_folder, return_only_tifs=False
            )
            # Should be only one zip
            try:
                downloaded_zip = [
                    i
                    for i in downloaded_files
                    if os.path.basename(i) == "lange2020_expected_occurrence.zip"
                ][0]
            except IndexError:
                self.log.error(
                    "after %s download - required zip file lange2020_expected_occurrence.zip was not present",
                    self.metadata.name,
                )
                raise Exception(f"{self.metadata.name} download failed")
            # Unpack zip
            unpack_zip(downloaded_zip, self.source_folder)
            # Moved nested tiffs up to source folder
            for tiff_fpath in tiffs_in_folder(
                os.path.join(self.source_folder, "lange2020_expected_occurrence"),
                full_paths=True,
            ):
                shutil.move(tiff_fpath, self.source_folder)
            shutil.rmtree(
                os.path.join(self.source_folder, "lange2020_expected_occurrence"),
                ignore_errors=True,
            )
            # Count the Tiffs
            self.log.debug("%s - Download Complete", self.metadata.name)
            assert (
                self._all_source_exists()
            ), f"after {self.metadata.name} download - not all source files were present"
            # Filter to just the files we support
            return [
                os.path.join(self.source_folder, _file) for _file in self.source_files
            ]

    def _all_source_exists(self, remove_invalid=True) -> bool:
        """
        Check if all source files exist and are valid
            If not source will be removed
        """
        source_valid = [True for _ in range(len(self.source_files))]
        for idx, _file in enumerate(self.source_files):
            fpath = os.path.join(self.source_folder, _file)
            try:
                assert_geotiff(fpath, check_compression=False, check_crs=None)
            except Exception as err:
                # remove the file and flag we should need to re-fetch, then move on
                self.log.warning(
                    "%s source file %s appears to be invalid due to %s",
                    self.metadata.name,
                    fpath,
                    err,
                )
                if remove_invalid:
                    if os.path.exists(fpath):
                        os.remove(fpath)
                source_valid[idx] = False
        return all(source_valid)
