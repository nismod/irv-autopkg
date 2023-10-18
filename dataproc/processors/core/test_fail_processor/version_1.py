"""
Test Failing Processor
"""

from time import sleep

from dataproc import DataPackageLicense
from dataproc.processors.internal.base import (
    BaseProcessorABC,
    BaseMetadataABC,
)


class Metadata(BaseMetadataABC):
    description = "A test processor that fails"  # Longer processor description
    dataset_name = ""  # The dataset this processor targets
    data_author = ""
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
    """A Test Failing Processor"""

    def generate(self):
        """Generate files for a given processor"""
        # Pause to allow inspection
        sleep(1)
        self.update_progress(30, "waiting")
        assert 0 == 1, "test-fail-processor failed as expected"
        return self.provenance_log

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
        return self.storage_backend.processor_file_exists(
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
            f"{self.boundary['name']}_test.tif",
        )
