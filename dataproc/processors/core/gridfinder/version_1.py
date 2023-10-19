"""
Gridfinder Processor
"""

import os
from typing import List

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    crop_raster,
    assert_geotiff,
    datapackage_resource,
    fetch_zenodo_doi,
    fiona_crop_file_to_geopkg,
    assert_vector_file,
)


class Metadata(BaseMetadataABC):
    description = "gridfinder - Predictive mapping of the global power system using open data"  # Longer processor description
    dataset_name = "gridfinder"  # The dataset this processor targets
    data_author = "Arderne, Christopher; Nicolas, Claire; Zorn, Conrad; Koks, Elco E"
    data_title = "Gridfinder"
    data_title_long = "Gridfinder data from 'Predictive mapping of the global power system using open data'"
    data_summary = """
Three primary global data outputs from the research:

grid.gpkg: Vectorized predicted distribution and transmission line network, with existing OpenStreetMap lines tagged in the 'source' column
targets.tif: Binary raster showing locations predicted to be connected to distribution grid.
lv.tif: Raster of predicted low-voltage infrastructure in kilometres per cell.

This data was created with code in the following three repositories:

https://github.com/carderne/gridfinder
https://github.com/carderne/predictive-mapping-global-power
https://github.com/carderne/access-estimator

Full steps to reproduce are contained in this file:

https://github.com/carderne/predictive-mapping-global-power/blob/master/README.md

The data can be visualized at the following location:

https://gridfinder.org
    """
    data_citation = """
Arderne, Christopher, Nicolas, Claire, Zorn, Conrad, & Koks, Elco E. (2020).
Data from: Predictive mapping of the global power system using open data [Data
set]. In Nature Scientific Data (1.1.1, Vol. 7, Number Article 19). Zenodo.
https://doi.org/10.5281/zenodo.3628142
"""
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://doi.org/10.5281/zenodo.3628142"
    data_formats = ["Geopackage", "GeoTIFF"]


class Processor(BaseProcessorABC):
    """A Processor for Gridfinder"""

    zenodo_doi = "10.5281/zenodo.3628142"
    source_files = ["grid.gpkg", "targets.tif", "lv.tif"]
    total_expected_files = len(source_files)

    def generate(self):
        """Generate files for a given processor"""

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
                self.output_filename(
                    self.metadata.name,
                    self.metadata.version,
                    self.boundary["name"],
                    file_format,
                    dataset_subfilename=subfilename,
                ),
            )
            if file_format == ".tif":
                crop_success = crop_raster(source_fpath, output_fpath, self.boundary)
            elif file_format == ".gpkg":
                crop_success = fiona_crop_file_to_geopkg(
                    source_fpath,
                    self.boundary,
                    output_fpath,
                    output_schema={
                        "properties": {"source": "str"},
                        "geometry": "LineString",
                    },
                    output_crs=4326,
                )
            else:
                continue
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
            "mixed",
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
            _ = fetch_zenodo_doi(self.zenodo_doi, self.source_folder)
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
            if os.path.splitext(_file)[1] == ".gpkg":
                try:
                    assert_vector_file(fpath)
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
            elif os.path.splitext(_file)[1] == ".tif":
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
            else:
                continue
        return all(source_valid)
