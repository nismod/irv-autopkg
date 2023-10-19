"""
Vector Processor for WRI Global Powerplants
"""

import os

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    data_file_hash,
    download_file,
    datapackage_resource,
    unpack_zip,
    csv_to_gpkg,
    fiona_crop_file_to_geopkg,
    assert_vector_file,
)


class Metadata(BaseMetadataABC):
    description = "World Resources Institute - Global Powerplants"
    dataset_name = "wri_powerplants"
    data_author = "World Resources Institute"
    data_title = "WRI Global Power Plant Database"
    data_title_long = "World Resources Institute Global Power Plant Database"
    data_summary = """The Global Power Plant Database is a comprehensive, open source database of power plants around the world. It
centralizes power plant data to make it easier to navigate, compare and draw insights for one’s own analysis.
The database covers approximately 35,000 power plants from 167 countries and includes thermal plants (e.g. coal,
gas, oil, nuclear, biomass, waste, geothermal) and renewables (e.g. hydro, wind, solar). Each power plant is
geolocated and entries contain information on plant capacity, generation, ownership, and fuel type. It will be
continuously updated as data becomes available."""
    data_citation = """Global Energy Observatory, Google, KTH Royal Institute of Technology in Stockholm, Enipedia, World Resources
Institute. 2018. Global Power Plant Database. Published on Resource Watch and Google Earth Engine;
http://resourcewatch.org/ https://earthengine.google.com/"""
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "https://datasets.wri.org/dataset/globalpowerplantdatabase"
    data_formats = ["Geopackage"]


class Processor(BaseProcessorABC):
    """A Processor for GRI OSM Database"""

    total_expected_files = 1
    source_zip_url = "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"
    expected_zip_hash = "083f11452efc1ed0e8fb1494f0ce49e5c37718e2"
    source_file = "global_power_plant_database.gpkg"
    output_schema = {
        "properties": {
            "country": "str",
            "country_long": "str",
            "name": "str",
            "gppd_idnr": "str",
            "capacity_mw": "float",
            "latitude": "float",
            "longitude": "float",
            "primary_fuel": "str",
            "other_fuel1": "str",
            "other_fuel2": "str",
            "other_fuel3": "str",
            "commissioning_year": "float",
            "owner": "str",
            "source": "str",
            "url": "str",
            "geolocation_source": "str",
            "wepp_id": "str",
            "year_of_capacity_data": "float",
            "generation_gwh_2013": "float",
            "generation_gwh_2014": "float",
            "generation_gwh_2015": "float",
            "generation_gwh_2016": "float",
            "generation_gwh_2017": "float",
            "generation_gwh_2018": "float",
            "generation_gwh_2019": "float",
            "generation_data_source": "str",
            "estimated_generation_gwh_2013": "float",
            "estimated_generation_gwh_2014": "float",
            "estimated_generation_gwh_2015": "float",
            "estimated_generation_gwh_2016": "float",
            "estimated_generation_gwh_2017": "float",
            "estimated_generation_note_2013": "str",
            "estimated_generation_note_2014": "str",
            "estimated_generation_note_2015": "str",
            "estimated_generation_note_2016": "str",
            "estimated_generation_note_2017": "str",
        },
        "geometry": "Point",
    }
    expected_source_gpkg_shape = (34936, 37)

    def output_filenames(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        return [
            self.output_filename(
                self.metadata.name, self.metadata.version, self.boundary["name"], "gpkg"
            )
        ]

    def generate(self):
        """Generate files for a given processor"""

        output_fpath = os.path.join(
            self.tmp_processing_folder,
            self.output_filename(
                self.metadata.name, self.metadata.version, self.boundary["name"], "gpkg"
            ),
        )

        # Fetch source as required
        self.update_progress(10, "fetching and verifying source")
        source_gpkg_fpath = self._fetch_source()

        # Crop to given boundary
        self.update_progress(50, "cropping source")
        self.log.debug("%s - cropping to geopkg", self.metadata.name)
        crop_result = fiona_crop_file_to_geopkg(
            source_gpkg_fpath, self.boundary, output_fpath, self.output_schema
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
        hashes, sizes = self.calculate_files_metadata([output_fpath])
        datapkg = datapackage_resource(
            self.metadata, [result_uri], "GEOPKG", sizes, hashes
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug(
            "%s generated datapackage in log: %s", self.metadata.name, datapkg
        )
        # Cleanup as required
        return self.provenance_log

    def _fetch_source(self) -> str:
        """
        Fetch, unpack and convert the source CSV to a geopkg for later processing

        return fpath str The path to the fetched source file
        """
        # Build Source Path
        os.makedirs(self.source_folder, exist_ok=True)
        if self._all_source_exists():
            self.log.debug(
                "%s - all source files appear to exist and are valid",
                self.metadata.name,
            )
            return os.path.join(self.source_folder, self.source_file)
        # Fetch the source zip
        self.log.debug("%s - fetching zip", self.metadata.name)
        local_zip_fpath = self._fetch_zip()
        self.log.debug("%s - fetched zip to %s", self.metadata.name, local_zip_fpath)
        self.provenance_log[
            f"{self.metadata.name} - zip download path"
        ] = local_zip_fpath
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
            "%s - CSV conversion to source GPKG success: %s",
            self.metadata.name,
            converted,
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
