

from abc import ABC

from dataproc.backends import StorageBackend
from dataproc import Boundary, DataPackageLicense

class BaseMetadataABC(ABC):
    """Base Metadata ABC"""

    name: str = "" # this must follow snakecase formatting, without special chars
    description: str = ""  # Longer processor description
    version: str = "" # Version of the Processor
    dataset_name: str = ""  # The dataset this processor targets
    data_author: str = ""
    data_license: DataPackageLicense = None
    data_origin_url: str = ""

class BaseProcessorABC(ABC):
    """Base Processor ABC"""

    def __init__(self, boundary: Boundary, storage_backend: StorageBackend) -> None:
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.provenance_log = {}
        self.paths_helper = None
        self.provenance_log = {}
        self.log = None
        # Source folder will persist between processor runs
        self.source_folder = None
        # Tmp Processing data will be cleaned
        self.tmp_processing_folder = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup any resources as required"""

    def generate(self):
        """Generate files for a given processor"""

    def exists(self):
        """Whether all files for a given processor exist on the FS on not"""

    def generate_datapackage(self):
        """Generate the datapackage resource for this processor
        and append to processor log
        """

    def generate_documentation(self):
        """Generate documentation for the processor
        on the result backend"""

    def _generate_index_file(self) -> str:
        """
        Generate the index documentation file

        ::returns dest_fpath str Destination filepath on the processing backend
        """

    def _generate_license_file(self) -> str:
        """
        Generate the License documentation file

        ::returns dest_fpath str Destination filepath on the processing backend
        """
