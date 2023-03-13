"""
Vector Processor for OSM Roads and Rail (inc. damages)
"""

import os
import inspect

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import (
    version_name_from_file,
    crop_osm_to_geopkg,
    data_file_hash,
    data_file_size,
    processor_name_from_file,
    generate_index_file,
    generate_license_file,
    generate_datapackage,
    output_filename
)
from config import (
    get_db_uri_ogr
)


class Metadata(BaseMetadataABC):
    """"""""

    name = processor_name_from_file(inspect.stack()[1].filename)  # this must follow snakecase formatting, without special chars
    description = (
        "Extraction from GRI OSM Table for Roads and Rail, including Damages"  # Longer processor description
    )
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = (
        "gri_osm_road_and_rail"  # The dataset this processor targets
    )
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
    data_origin_url = (
        "https://global.infrastructureresilience.org"
    )


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
    output_geometry_operation = "clip" # Clip or intersect
    osm_crop_batch_size = 1000

    def exists(self):
        """Whether all output files for a given processor & boundary exist on the FS on not"""
        return self.storage_backend.processor_file_exists(
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
            output_filename(self.metadata.name, self.metadata.version, self.boundary["name"], 'gpkg')
        )

    def generate(self):
        """Generate files for a given processor"""
        if self.exists() is True:
            self.provenance_log[self.metadata.name] = "exists"
            return self.provenance_log
        # Setup output path in the processing backend
        output_fpath = os.path.join(
            self.tmp_processing_folder, 
            output_filename(self.metadata.name, self.metadata.version, self.boundary["name"], 'gpkg')
        )

        # Crop to given boundary
        self.update_progress(10, "cropping source")
        self.log.debug("%s - cropping to geopkg", self.metadata.name)
        gen = crop_osm_to_geopkg(
            self.boundary,
            str(get_db_uri_ogr(
                dbname=os.getenv(self.pg_osm_dbname_env),
                username_env=self.pg_osm_user_env,
                password_env=self.pg_osm_password_env,
                host_env=self.pg_osm_host_env,
                port_env=self.pg_osm_port_env
            )),
            self.input_pg_table,
            output_fpath,
            geometry_column=self.input_geometry_column,
            extract_type=self.output_geometry_operation,
            batch_size=self.osm_crop_batch_size
        )
        while True:
            try:
                progress = next(gen)
                self.update_progress(10 + int((progress[1]/progress[0])*80), "cropping source")
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
        
        # Generate Datapackage
        hashes = [data_file_hash(output_fpath)]
        sizes = [data_file_size(output_fpath)]
        datapkg = generate_datapackage(
            self.metadata, [result_uri], "GEOPKG", sizes, hashes
        )
        self.provenance_log["datapackage"] = datapkg
        self.log.debug("%s generated datapackage in log: %s", self.metadata.name, datapkg)
        # Cleanup as required
        os.remove(output_fpath)
        return self.provenance_log

    def generate_documentation(self):
        """Generate documentation for the processor
        on the result backend"""
        # Generate Documentation
        index_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "templates", self.metadata.version, self.index_filename
        )
        index_create = generate_index_file(
            self.storage_backend,
            index_fpath, 
            self.boundary["name"],
            self.metadata
        )
        self.provenance_log[f"{self.metadata.name} - created index documentation"] = index_create
        license_fpath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "templates", self.metadata.version, self.license_filename
        )
        license_create = generate_license_file(
            self.storage_backend,
            license_fpath, 
            self.boundary["name"],
            self.metadata
        )
        self.provenance_log[f"{self.metadata.name} - created license documentation"] = license_create
        self.log.debug("%s generated documentation on backend", self.metadata.name)
