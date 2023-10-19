"""
STORM (Global Mosaics Version 1) Processor
"""

import os
from typing import List

from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)
from dataproc import DataPackageLicense
from dataproc.helpers import (
    crop_raster,
    assert_geotiff,
    datapackage_resource,
    fetch_zenodo_doi,
    tiffs_in_folder,
)


class Metadata(BaseMetadataABC):
    description = "A Processor for WRI Aqueduct"  # Longer processor description
    dataset_name = "STORM Global Mosaics 10.5281/zenodo.7438145"  # The dataset this processor targets
    data_author = "University of Oxford"
    data_title = "STORM tropical cyclone wind speed maps"
    data_title_long = (
        "STORM tropical cyclone wind speed return period maps as global GeoTIFFs"
    )
    data_summary = """
Global tropical cyclone wind speed return period maps.

This dataset is derived with minimal processing from the following datasets
created by Bloemendaal et al, which are released with a CC0 license:

[1] Bloemendaal, Nadia; de Moel, H. (Hans); Muis, S; Haigh, I.D. (Ivan); Aerts,
J.C.J.H. (Jeroen) (2020): STORM tropical cyclone wind speed return periods.
4TU.ResearchData. Dataset. https://doi.org/10.4121/12705164.v3

[2] Bloemendaal, Nadia; de Moel, Hans; Dullaart, Job; Haarsma, R.J. (Reindert);
Haigh, I.D. (Ivan); Martinez, Andrew B.; et al. (2022): STORM climate change
tropical cyclone wind speed return periods. 4TU.ResearchData. Dataset.
https://doi.org/10.4121/14510817.v3

Datasets containing tropical cyclone maximum wind speed (in m/s) return periods,
generated using the STORM datasets (see
https://www.nature.com/articles/s41597-020-0381-2) and STORM climate change
datasets (see https://figshare.com/s/397aff8631a7da2843fc). Return periods were
empirically calculated using Weibull's plotting formula. The
STORM_FIXED_RETURN_PERIOD dataset contains maximum wind speeds for a fixed set
of return periods at 10 km resolution in every basin and for every climate model
used here (see below).

The GeoTIFFs provided in the datasets linked above have been mosaicked into
single files with global extent for each climate model/return period using the
following code:

https://github.com/nismod/open-gira/blob/219315e57cba54bb18f033844cff5e48dd5979d7/workflow/rules/download/storm-ibtracs.smk#L126-L151

Files are named on the pattern:
STORM_FIXED_RETURN_PERIODS_{STORM_MODEL}_{STORM_RP}_YR_RP.tif

STORM_MODEL is be one of constant, CMCC-CM2-VHR4, CNRM-CM6-1-HR, EC-Earth3P-HR
or HadGEM3-GC31-HM. The "constant" files are for the present day, baseline
climate scenario as explained in dataset [1]. The other files are for 2050,
RCP8.5 under different models as explained in the paper linked from dataset [2].

STORM_RP is one of 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500,
600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000 or
10000.
"""
    data_citation = """
Russell, Tom. (2022). STORM tropical cyclone wind speed return periods as global
GeoTIFFs (1.0.0) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.7438145

Derived from:

[1] Bloemendaal, Nadia; de Moel, H. (Hans); Muis, S; Haigh, I.D. (Ivan); Aerts,
J.C.J.H. (Jeroen) (2020): STORM tropical cyclone wind speed return periods.
4TU.ResearchData. Dataset. https://doi.org/10.4121/12705164.v3

[2] Bloemendaal, Nadia; de Moel, Hans; Dullaart, Job; Haarsma, R.J. (Reindert);
Haigh, I.D. (Ivan); Martinez, Andrew B.; et al. (2022): STORM climate change
tropical cyclone wind speed return periods. 4TU.ResearchData. Dataset.
https://doi.org/10.4121/14510817.v3
    """
    data_license = DataPackageLicense(
        name="CC0",
        title="CC0",
        path="https://creativecommons.org/share-your-work/public-domain/cc0/",
    )
    data_origin_url = "https://doi.org/10.5281/zenodo.7438145"
    data_formats = ["GeoTIFF"]


class Processor(BaseProcessorABC):
    """A Processor for STORM Global Mosaics"""

    zenodo_doi = "10.5281/zenodo.7438145"
    total_expected_files = 140

    def generate(self):
        """Generate files for a given processor"""

        # Check if the source TIFF exists and fetch it if not
        self.update_progress(10, "fetching and verifying source")
        source_fpaths = self._fetch_source()

        self.log.debug("%s - cropping geotiffs", self.metadata.name)
        results_fpaths = []
        for idx, source_fpath in enumerate(source_fpaths):
            self.update_progress(
                10 + int(idx * (80 / len(source_fpaths))), "cropping source"
            )
            subfilename = os.path.splitext(os.path.basename(source_fpath))[0]
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
            "GeoTiFF",
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
            return tiffs_in_folder(self.source_folder, full_paths=True)
        else:
            source_fpaths = fetch_zenodo_doi(self.zenodo_doi, self.source_folder)
            # Count the Tiffs
            self.log.debug("%s - Download Complete", self.metadata.name)
            assert (
                len(source_fpaths) == self.total_expected_files
            ), f"after {self.metadata.name} download - not all source files were present"
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
                        self.metadata.name,
                    )
                    if remove_invalid:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                    source_valid = False
        return source_valid and (count_tiffs == self.total_expected_files)
