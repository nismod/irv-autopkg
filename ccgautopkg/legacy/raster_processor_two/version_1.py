"""
Test Raster Processor
"""

from time import sleep

from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC


class Metadata(BaseMetadataABC):
    """Processor metadata"""
    name="raster_processor_two"
    description = "Raster Processor Two"
    dataset_name = "A raster dataset named Two"
    data_author = "An Author"
    data_license = "License"
    data_origin_url = "http://url"
    version = "1"


class RasterProcessorTwo(BaseProcessorABC):
    """A Raster Processor"""

    def __init__(
        self,
        boundary: Boundary,
        storage_backend: StorageBackend,
        processing_backend: ProcessingBackend,
    ) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend

    def generate(self):
        """Generate files for a given processor"""
        sleep(1)
        return True

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
