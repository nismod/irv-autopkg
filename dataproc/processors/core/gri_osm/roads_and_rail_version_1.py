"""
Vector Processor for OSM Roads and Rail (inc. damages)
"""

import os
from typing import List

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    crop_osm_to_geopkg,
    datapackage_resource,
)
from dataproc.exceptions import ProcessorDatasetExists
from config import get_db_uri_ogr, PACKAGES_HOST_URL


class Metadata(BaseMetadataABC):
    description = "Extraction from GRI OSM Table for Roads and Rail, including Damages"  # Longer processor description
    dataset_name = "gri_osm_road_and_rail"  # The dataset this processor targets
    data_author = "nismod/open-gira contributors and OpenStreetMap contributors"
    data_title = "Road and Rail networks derived from OpenStreetMap"
    data_title_long = "Road and Rail networks derived from OpenStreetMap"
    data_summary = """
OpenStreetMap provides map data, including on road and railway networks.
This dataset is a derived, processed extract from the global OpenStreetMap
database, produced by researchers at the University of Oxford to support
infrastructure systems analysis and climate risk and resilience assessments.

The data is produced from a snapshot of OpenStreetMap (the current version is
taken from November 2022) by a reproducible pipeline which is under development
and made freely available at https://github.com/nismod/open-gira.
    """
    data_citation = """
Russell T., Thomas F., nismod/open-gira contributors and OpenStreetMap contributors (2022)
Global Road and Rail networks derived from OpenStreetMap. [Dataset] Available at https://global.infrastructureresilience.org
    """
    data_license = DataPackageLicense(
        name="ODbL-1.0",
        title="Open Data Commons Open Database License 1.0",
        path="https://opendefinition.org/licenses/odc-odbl",
    )
    data_origin_url = "https://global.infrastructureresilience.org"
    data_formats = ["Geopackage"]


class Processor(BaseProcessorABC):
    """A Processor for GRI OSM Database"""

    index_filename = "index.html"
    license_filename = "license.html"

    pg_osm_host_env = "AUTOPKG_OSM_PGHOST"
    pg_osm_port_env = "AUTOPKG_OSM_PORT"
    pg_osm_user_env = "AUTOPKG_OSM_PGUSER"
    pg_osm_password_env = "AUTOPKG_OSM_PGPASSWORD"
    pg_osm_dbname_env = "AUTOPKG_OSM_PGDATABASE"
    input_pg_table = "features"
    input_geometry_column = "geom"
    output_geometry_operation = "clip"  # Clip or intersect
    osm_crop_batch_size = 1000

    def output_filenames(self) -> List[str]:
        return [
            self.output_filename(
                self.metadata.name,
                self.metadata.version,
                self.boundary["name"],
                "gpkg",
            )
        ]

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            raise ProcessorDatasetExists()
        # Setup output path in the processing backend
        output_fpath = os.path.join(
            self.tmp_processing_folder, self.output_filenames()[0]
        )

        # Crop to given boundary
        self.update_progress(10, "cropping source")
        self.log.debug("%s - cropping to geopkg", self.metadata.name)
        gen = crop_osm_to_geopkg(
            self.boundary,
            get_db_uri_ogr(
                dbname=os.getenv(self.pg_osm_dbname_env, ""),
                username_env=self.pg_osm_user_env,
                password_env=self.pg_osm_password_env,
                host_env=self.pg_osm_host_env,
                port_env=self.pg_osm_port_env,
            ).__str__(),
            self.input_pg_table,
            output_fpath,
            geometry_column=self.input_geometry_column,
            extract_type=self.output_geometry_operation,
            batch_size=self.osm_crop_batch_size,
        )
        while True:
            try:
                progress = next(gen)
                self.update_progress(
                    10 + int((progress[1] / progress[0]) * 80), "cropping source"
                )
            except StopIteration:
                break
        self.provenance_log[f"{self.metadata.name} - crop completed"] = True
        # Move cropped data to backend
        self.update_progress(90, "moving result")
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

        datapkg = self.generate_datapackage_resource()
        self.provenance_log["datapackage"] = datapkg
        self.log.debug(
            "%s generated datapackage in log: %s", self.metadata.name, datapkg
        )
        # Cleanup as required
        os.remove(output_fpath)
        return self.provenance_log

    def generate_datapackage_resource(self):
        output_fpath = os.path.join(
            self.tmp_processing_folder, self.output_filenames()[0]
        )
        ## TODO write helpers for files produced (local path) and resulting URIs
        hashes, sizes = self.calculate_files_metadata([output_fpath])
        result_uri = output_fpath.replace(self.tmp_processing_folder, PACKAGES_HOST_URL)
        uris = [result_uri]
        return datapackage_resource(self.metadata, uris, "GPKG", sizes, hashes)
