import os
import logging
import shutil
from abc import ABC, abstractmethod

from celery.app import task

from dataproc.storage import StorageBackend
from dataproc import Boundary, DataPackageLicense


class BaseMetadataABC(ABC):
    """Base Metadata ABC"""

    name: str = ""  # this must follow snakecase formatting, without special chars
    description: str = ""  # Longer processor description
    version: str = ""  # Version of the Processor
    dataset_name: str = ""  # The dataset this processor targets
    data_title: str = ""  # Short one-liner title for dataset, ~30 characters is good
    data_title_long: str = ""  # Long title for dataset
    data_author: str = ""
    data_summary: str = ""  # 1-3 paragraph prose summary of the dataset
    data_citation: str = ""  # Suggested citation, e.g. "Nicholas, C (2023) irv-autopkg. [Software] Available at: https://github.com/nismod/irv-autopkg"
    data_license: DataPackageLicense = None
    data_origin_url: str = ""


class BaseProcessorABC(ABC):
    """Base Processor ABC"""

    def __init__(
        self,
        metadata: BaseMetadataABC,
        boundary: Boundary,
        storage_backend: StorageBackend,
        task_executor: task,
        processing_root_folder: str,
    ) -> None:
        """Processor instatntiation"""
        self.metadata = metadata
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.executor = task_executor
        self.provenance_log = {}
        self.log = logging.getLogger(__name__)
        # Source folder will persist between processor runs
        self.processing_root_folder = processing_root_folder
        self.source_folder = os.path.join(self.processing_root_folder, "source_data")
        os.makedirs(self.source_folder, exist_ok=True)
        # Tmp Processing data will be cleaned between processor runs
        self.tmp_processing_folder = os.path.join(
            self.processing_root_folder, "tmp", self.boundary["name"]
        )
        os.makedirs(self.tmp_processing_folder, exist_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup any resources as required"""
        self.log.debug(
            "cleaning processing data on exit, exc: %s, %s, %s",
            exc_type,
            exc_val,
            exc_tb,
        )
        try:
            shutil.rmtree(self.tmp_processing_folder, ignore_errors=True)
        except FileNotFoundError:
            pass

    def update_progress(
        self,
        percent_complete: int,
        current_task: str,
    ):
        """Update external executor with Processor progress"""
        if self.executor:
            try:
                self.executor.update_state(
                    state="EXECUTING",
                    meta={
                        f"{self.metadata.name}.{self.metadata.version}": {
                            "progress": percent_complete,
                            "current_task": current_task,
                        }
                    },
                )
            except Exception as err:
                self.log.warning(
                    "%s.%s failed to update progress state due to %s",
                    self.metadata.name,
                    self.metadata.version,
                    err,
                )

    @staticmethod
    def output_filename(
        dataset_name: str,
        dataset_version: str,
        boundary_name: str,
        file_format: str,
        dataset_subfilename: str = None,
    ) -> str:
        """
        Generate a standardized output filename
        """
        base = f"{dataset_name}-{dataset_version}"
        if not dataset_subfilename:
            return f"{base}-{boundary_name}.{file_format.replace('.', '')}"
        else:
            return f"{base}-{dataset_subfilename}-{boundary_name}.{file_format.replace('.', '')}"

    @abstractmethod
    def output_fpaths(self) -> list[str]:
        """List all expected files for a given processor"""

    def exists(self):
        """Whether all output files for a given processor and boundary exist"""
        return self.storage_backend.processor_file_exists(
            self.boundary["name"],
            self.metadata.name,
            self.metadata.version,
            self.output_filename(
                self.metadata.name, self.metadata.version, self.boundary["name"], "gpkg"
            ),
        )

    @abstractmethod
    def generate(self):
        """Generate files for a given processor"""

    @abstractmethod
    def generate_datapackage_resource(self):
        """Generate datapackage resource for a given processor"""
