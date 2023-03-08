"""
Test vector Processor
"""

import os
import inspect
from typing import List

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
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
    fiona_crop_file_to_geopkg,
    assert_vector_file,
    output_filename
)


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(
        inspect.stack()[1].filename
    )  # this must follow snakecase formatting, without special chars
    description = "gridfinder - Predictive mapping of the global power system using open data"  # Longer processor description
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "gridfinder"  # The dataset this processor targets
    data_author = "Arderne, Christopher; NIcolas, Claire; Zorn, Conrad; Koks, Elco E"
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://doi.org/10.5281/zenodo.3628142"


class Processor(BaseProcessorABC):
    """A Processor for Gridfinder"""

    zenodo_doi = "10.5281/zenodo.3628142"
    source_files = ["grid.gpkg", "targets.tif", "lv.tif"]
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
                    dataset_subfilename=subfilename
                )
            )
            if file_format == ".tif":
                crop_success = crop_raster(
                    source_fpath, output_fpath, self.boundary, preserve_raster_crs=True
                )
            elif file_format == ".gpkg":
                crop_success = fiona_crop_file_to_geopkg(
                    source_fpath,
                    self.boundary,
                    output_fpath,
                    output_schema = {'properties': {'source': 'str'}, 'geometry': 'LineString'},
                    output_crs=4326
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
        self.log.debug("%s - moving cropped data to backend", self.metadata.name)
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

        # Generate documentation on backend
        self.update_progress(90, "generate documentation & datapackage")
        self.generate_documentation()

        # Generate datapackage in log (using directory for URI)
        datapkg = generate_datapackage(
            self.metadata,
            result_uris,
            "mixed",
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
