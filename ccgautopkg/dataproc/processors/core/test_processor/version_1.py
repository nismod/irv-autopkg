"""
Test Raster Processor
"""

from time import sleep

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC

class Metadata(BaseMetadataABC):
    """Processor metadata"""
    name="test_processor" # this must follow snakecase formatting, without special chars
    description="A test processor for nightlights" # Longer processor description
    version="1" # Version of the Processor
    dataset_name="nightlights" # The dataset this processor targets
    data_author="Nightlights Author"
    data_license="Nightlights License"
    data_origin_url="http://url"

class Processor(BaseProcessorABC):
    """A Processor for Nightlights"""

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend, processing_backend: ProcessingBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend

    def generate(self):
        """Generate files for a given processor"""
        # Pause to allow inspection
        sleep(5)
        return True

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
        pass