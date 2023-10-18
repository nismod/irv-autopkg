import os
from abc import ABC, abstractmethod
from typing import List


class StorageBackend(ABC):
    """StorageBackend base class"""

    datasets_folder_name = "datasets"
    dataset_data_folder_name = "data"

    @abstractmethod
    def package_datasets(self, package: str) -> List[str]:
        pass

    @abstractmethod
    def dataset_versions(self, package: str, dataset: str) -> List[str]:
        pass

    @abstractmethod
    def put_processor_data(
        self,
        local_source_fpath: str,
        boundary_name: str,
        dataset_name: str,
        version: str,
        remove_local_source=False,
    ) -> str:
        """
        Put data output from a processor for a particular dataset and
        version onto the backend

        ::kwarg remove_local_source bool Whether to delete the local source file
            after a successful move

        ::returns dest_abs_path str URI of the moved file
        """

    @abstractmethod
    def put_processor_metadata(
        self,
        local_source_fpath: str,
        boundary_name: str,
        dataset_name: str,
        version: str,
        remove_local_source=False,
    ) -> str:
        """
        Put an a processor metadata file for a particular dataset and
        version onto the backend

        ::kwarg remove_local_source bool Whether to delete the local source file
            after a successful move

        ::returns dest_abs_path str URI of the moved file
        """


class PathsHelper:
    """Helpers for management of paths on the processing backend"""

    def __init__(self, top_level_folder_path: str) -> None:
        self.top_level_folder_path = top_level_folder_path

    def build_absolute_path(self, *relative_path_components) -> str:
        """
        Build an absolute path from a relative path, by pre-pending the configured top level directory
        """
        return os.path.join(self.top_level_folder_path, *relative_path_components)
