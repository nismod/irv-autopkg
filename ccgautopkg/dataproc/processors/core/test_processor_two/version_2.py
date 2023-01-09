"""
Test Raster Processor
"""

from time import sleep

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC

class Metadata(BaseMetadataABC):
    """Processor metadata"""
    name="test_processor_two" # this must follow snakecase formatting, without special chars
    description="A test processor for _two" # Longer processor description
    version="2" # Version of the Processor
    dataset_name="_two" # The dataset this processor targets
    data_author="_two Author"
    data_license="_two License"
    data_origin_url="http://url"

class Processor(BaseProcessorABC):
    """A Processor for _two"""

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend, processing_backend: ProcessingBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend
        self.provenance_log = {}

    def generate(self):
        """Generate files for a given processor"""
        # Pause to allow inspection
        sleep(1)
        self.provenance_log['test_processor_two_v2_completed'] = True
        return self.provenance_log

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
        pass