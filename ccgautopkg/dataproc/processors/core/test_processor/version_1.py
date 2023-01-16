"""
Test Raster Processor
"""

from time import sleep
import logging
import os
import inspect

from dataproc.backends import StorageBackend
from dataproc.backends.base import PathsHelper
from dataproc import Boundary
from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc.helpers import version_name_from_file, create_test_file
from config import LOCALFS_PROCESSING_BACKEND_ROOT


class Metadata(BaseMetadataABC):
    """Processor metadata"""

    name = (
        "test_processor"  # this must follow snakecase formatting, without special chars
    )
    description = "A test processor for nightlights"  # Longer processor description
    version = version_name_from_file(
        inspect.stack()[1].filename
    )  # Version of the Processor
    dataset_name = "nightlights"  # The dataset this processor targets
    data_author = "Nightlights Author"
    data_license = "Nightlights License"
    data_origin_url = "http://url"


class Processor(BaseProcessorABC):
    """A Processor for Nightlights"""

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.paths_helper = PathsHelper(os.path.join(LOCALFS_PROCESSING_BACKEND_ROOT, Metadata().name))
        self.provenance_log = {}
        self.log = logging.getLogger(__name__)

    def generate(self):
        """Generate files for a given processor"""
        # Pause to allow inspection
        sleep(1)
        output_folder = self.paths_helper.build_absolute_path(
            "test_processor", Metadata().version, "outputs"
        )
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}_test.tif")
        if self.exists() is True:
            self.provenance_log[f"{Metadata().name}"] = "exists"
        else:
            # Generate a blank tests dataset
            create_test_file(output_fpath)
            result_uri = self.storage_backend.put_processor_data(
                output_fpath,
                self.boundary["name"],
                Metadata().name,
                Metadata().version,
            )
            self.provenance_log[f"{Metadata().name} - move to storage success"] = True
            self.provenance_log[f"{Metadata().name} - result URI"] = result_uri
        return self.provenance_log

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""
        output_folder = self.paths_helper.build_absolute_path(
            "test_processor", Metadata().version, "outputs"
        )
        output_fpath = os.path.join(output_folder, f"{self.boundary['name']}_test.tif")
        return os.path.exists(output_fpath)
