"""
Vector Processor for WRI Global Powerplants
"""

import os
import inspect

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    version_name_from_file,
    download_file,
    data_file_hash,
    data_file_size,
    processor_name_from_file,
    generate_index_file,
    generate_license_file,
    generate_datapackage,
    unpack_zip,
    csv_to_gpkg,
    gp_crop_file_to_geopkg,
    assert_vector_file,
)


class Metadata(BaseMetadataABC):
    """
    Processor metadata
    """

    name = processor_name_from_file(inspect.stack()[1].filename)
    description = "World Resources Institute - Global Powerplants"
    version = version_name_from_file(inspect.stack()[1].filename)
    dataset_name = "wri_powerplants"
    data_author = "World Resources Institute"
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://datasets.wri.org/dataset/globalpowerplantdatabase"


class Processor(BaseProcessorABC):
    """A Processor for GRI OSM Database"""

    index_filename = "index.html"
    license_filename = "license.html"
    total_expected_files = 1
    source_zip_url = "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"
    expected_zip_hash = "083f11452efc1ed0e8fb1494f0ce49e5c37718e2"
    source_file = "global_power_plant_database.gpkg"
    expected_source_gpkg_shape = (34936, 37)

    def exists(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        return self.storage_backend.processor_file_exists(
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
            f"{self.boundary['name']}.gpkg",
        )

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            self.provenance_log[self.metadata.name] = "exists"
            return self.provenance_log
        # Setup output path in the processing backend
        output_folder = self.paths_helper.build_absolute_path(
            self.boundary["name"], self.metadata.name, self.metadata.version, "outputs"
        )
        os.makedirs(output_folder, exist_ok=True)
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}.gpkg")

        # Fetch source as required
        self.update_progress(10, "fetching and verifying source")
        source_gpkg_fpath = self._fetch_source()

        # Crop to given boundary
        self.update_progress(50, "cropping source")
        self.log.debug("%s - cropping to geopkg", self.metadata.name)
        crop_result = gp_crop_file_to_geopkg(
            source_gpkg_fpath, self.boundary, output_fpath, mask_type="boundary"
        )

        self.provenance_log[f"{self.metadata.name} - crop completed"] = crop_result
        # Move cropped data to backend
        self.update_progress(50, "moving result")
        self.log.debug("%s - moving cropped data to backend", {self.metadata.name})
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
        )
        self.provenance_log[f"{self.metadata.name} - move to storage success"] = True
        self.provenance_log[f"{self.metadata.name} - result URI"] = result_uri

        self.update_progress(80, "generate documentation & datapackage")
        self.generate_documentation()

        # Generate Datapackage
        hashes = [data_file_hash(output_fpath)]
        sizes = [data_file_size(output_fpath)]
        datapkg = generate_datapackage(
            self.metadata, [result_uri], "GEOPKG", sizes, hashes
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug("%s generated datapackage in log: %s", self.metadata.name, datapkg)
        # Cleanup as required
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

    def _fetch_source(self) -> str:
        """
        Fetch, unpack and convert the source CSV to a geopkg for later processing

        return fpath str The path to the fetched source file
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid", self.metadata.name
            )
            return os.path.join(self.source_folder, self.source_file)
        # Fetch the source zip
        self.log.debug("%s - fetching zip", self.metadata.name)
        local_zip_fpath = self._fetch_zip()
        self.log.debug("%s - fetched zip to %s", self.metadata.name, local_zip_fpath)
        self.provenance_log[f"{self.metadata.name} - zip download path"] = local_zip_fpath
        # Unpack
        self.log.debug("%s - unpacking zip", self.metadata.name)
        unpack_zip(local_zip_fpath, self.tmp_processing_folder)
        # Convert CSV to GPKG
        csv_fpath = os.path.join(
            self.tmp_processing_folder, "global_power_plant_database.csv"
        )
        gpkg_fpath = os.path.join(self.source_folder, self.source_file)
        # Load to geopkg
        self.log.debug("%s - converting source CSV to GPKG", self.metadata.name)
        converted = csv_to_gpkg(
            csv_fpath,
            gpkg_fpath,
            "EPSG:4326",
            latitude_col="latitude",
            longitude_col="longitude",
        )
        self.log.info(
            "%s - CSV conversion to source GPKG success: %s", self.metadata.name, converted
        )

        return gpkg_fpath

    def _fetch_zip(self) -> str:
        """
        Fetch the Source Zip File

        ::returns filepath str Result local filepath
        """
        # Pull the zip file to the configured processing backend
        zip_fpath = os.path.join(
            self.tmp_processing_folder, os.path.basename(self.source_zip_url)
        )
        zip_fpath = download_file(
            self.source_zip_url,
            zip_fpath,
        )
        assert (
            data_file_hash(zip_fpath) == self.expected_zip_hash
        ), f"{self.metadata.name} downloaded zip file hash did not match expected"
        return zip_fpath

    def _all_source_exists(self, remove_invalid=True) -> bool:
        """
        Check if all source files exist and are valid
            If not source will be removed
        """
        source_valid = True
        fpath = os.path.join(self.source_folder, self.source_file)
        try:
            assert_vector_file(
                fpath,
                expected_shape=self.expected_source_gpkg_shape,
                expected_crs="EPSG:4326",
            )
        except Exception as err:
            # remove the file and flag we should need to re-fetch, then move on
            self.log.warning(
                "%s source file %s appears to be invalid - due to %s",
                self.metadata.name,
                fpath,
                err,
            )
            if remove_invalid:
                if os.path.exists(fpath):
                    os.remove(fpath)
            source_valid = False
        return source_valid
