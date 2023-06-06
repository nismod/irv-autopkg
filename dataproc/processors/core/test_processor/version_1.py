"""
Test Raster Processor
"""

from time import sleep
import os
import inspect

from dataproc import DataPackageLicense
from dataproc.exceptions import ProcessorDatasetExists
from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)
from dataproc.helpers import (
    version_name_from_file,
    create_test_file,
    data_file_hash,
    datapackage_resource,
    processor_name_from_file,
)


class Metadata(BaseMetadataABC):
    """Processor metadata"""

    name = processor_name_from_file(
        inspect.stack()[1].filename
    )  # this must follow snakecase formatting, without special chars
    description = "A test processor for nightlights"  # Longer processor description
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
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
        output_folder = self.paths_helper.build_absolute_path(
            "test_processor", self.metadata.version, "outputs"
        )
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}_test.tif")
        if self.exists() is True:
            raise ProcessorDatasetExists()
        else:
            # Generate a blank tests dataset
            create_test_file(output_fpath)
            result_uri = self.storage_backend.put_processor_data(
                output_fpath,
                self.boundary["name"],
                self.metadata.name,
                self.metadata.version,
            )
            self.provenance_log[
                f"{self.metadata.name} - move to storage success"
            ] = True
            self.provenance_log[f"{self.metadata.name} - result URI"] = result_uri
            # Generate the datapackage and add it to the output log
            datapkg = datapackage_resource(
                self.metadata,
                [result_uri],
                "GEOPKG",
                [os.path.getsize(output_fpath)],
                [data_file_hash(output_fpath)],
            )
            self.provenance_log["datapackage"] = datapkg.asdict()
        return self.provenance_log

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
        return self.storage_backend.processor_file_exists(
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
            f"{self.boundary['name']}_test.tif",
        )
