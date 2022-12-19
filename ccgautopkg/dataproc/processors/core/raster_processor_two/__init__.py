"""
Test Raster Processor
"""

from time import sleep

from dataproc.backends import Backend
from dataproc.helpers import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC

class Metadata(BaseMetadataABC):
    """Processor metadata"""
    processor_name="Raster Processor Two"
    dataset_name="A raster dataset named Two"
    data_author="An Author"
    data_license="License"
    data_origin_url="http://url"

class RasterProcessorTwo(BaseProcessorABC):
    """A Raster Processor"""

    def __init__(self, boundary: Boundary, backend: Backend) -> None:
        self.boundary = boundary
        self.backend = backend

    def generate(self):
        """Generate files for a given processor"""
        sleep(1)
        return True

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
