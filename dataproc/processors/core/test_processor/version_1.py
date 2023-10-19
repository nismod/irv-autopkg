"""
Test Raster Processor
"""

from time import sleep
import os

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)
from dataproc.helpers import (
    create_test_file,
    datapackage_resource,
)


class Metadata(BaseMetadataABC):
    description = "A test processor for nightlights"  # Longer processor description
    dataset_name = "nightlights"  # The dataset this processor targets
    data_author = "Nightlights Author"
    data_title = ""
    data_title_long = ""
    data_summary = ""
    data_citation = ""
    data_license = DataPackageLicense(
        name="CC-BY-4.0",
        title="Creative Commons Attribution 4.0",
        path="https://creativecommons.org/licenses/by/4.0/",
    )
    data_origin_url = "http://url"
    data_formats = ["GeoTIFF"]


class Processor(BaseProcessorABC):
    """A Test Processor"""

    def generate(self):
        """Generate files for a given processor"""
        # Pause to allow inspection
        sleep(1)
        self.update_progress(30, "waiting")
        output_folder = os.path.join(
            self.processing_root_folder,
            "test_processor",
            self.metadata.version,
            "outputs",
        )
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}_test.tif")

        # Generate a blank tests dataset
        create_test_file(output_fpath)
        result_uri = self.storage_backend.put_processor_data(
            output_fpath,
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
        )
        self.provenance_log[f"{self.metadata.name} - move to storage success"] = True
        self.provenance_log[f"{self.metadata.name} - result URI"] = result_uri
        # Generate the datapackage and add it to the output log

        hashes, sizes = self.calculate_files_metadata([output_fpath])
        datapkg = datapackage_resource(
            self.metadata,
            [result_uri],
            "tiff",
            sizes,
            hashes,
        )
        self.provenance_log["datapackage"] = datapkg.asdict()

        return self.provenance_log

    def output_filenames(self):
        return [f"{self.boundary['name']}_test.tif"]
