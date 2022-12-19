"""
Test Raster Processor
"""

from time import sleep

from dataproc.backends import Backend
from dataproc.helpers import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC

class Metadata(BaseMetadataABC):
    """Processor metadata"""
    processor_name="Raster Processor One"
    dataset_name="A raster dataset named one"
    data_author="An Author"
    data_license="License"
    data_origin_url="http://url"

class RasterProcessorOne(BaseProcessorABC):
    """A Raster Processor"""

    def __init__(self, boundary: Boundary, backend: Backend) -> None:
        self.boundary = boundary
        self.backend = backend

    def generate(self):
        """Generate files for a given processor"""
        sleep(5)
        return True

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
