from abc import ABC, abstractmethod
from typing import List

from dataproc import DataPackageResource


class StorageBackend(ABC):
    """StorageBackend base class"""

    datasets_folder_name = "datasets"
    dataset_data_folder_name = "data"

    @abstractmethod
    def packages(self, summary: bool = False) -> List[str]:
        """List of Packages that currently exist under the top-level storage backend"""

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

    @abstractmethod
    def add_provenance(
        self,
        boundary_name: str,
        processing_log: List[dict],
        filename: str = "provenance.json",
    ) -> bool:
        """
        Generate new and/or append given processing log to a boundaries provenance file

        {
            "isoformat dtg": {log}, ...
        }
        """

    @abstractmethod
    def update_datapackage(self, boundary_name: str, dp_resource: DataPackageResource):
        """
        Update a packages datapackage.json file with details of a given dataset.

        __NOTE__: Assumes the Boundary processor has already run and datapackage exists (even as just template)
        """

    @abstractmethod
    def _build_absolute_path(self, *args) -> str:
        """
        Build an absolute path from a relative path, by pre-pending the configured top level directory
        """

    @abstractmethod
    def _build_uri(self, absolute_fpath: str) -> str:
        """Build the internet-accessible URI from a given localFS absolute fpath"""

    @abstractmethod
    def _exists(self, absolute_path: str) -> bool:
        """
        Check if an object at the given path exists
        """

    @abstractmethod
    def create_boundary_folder(self, boundary_name: str):
        """
        Create a boundary folder
        """

    @abstractmethod
    def create_boundary_data_folder(self, boundary_name: str):
        """
        Create a boundary data folder
        """

    def boundary_folder_exists(self, boundary_name: str):
        """If a given boundary folder exists"""
        return self._exists(self._build_absolute_path(boundary_name))

    def boundary_data_folder_exists(self, boundary_name: str):
        """If a given boundary data folder exists"""
        return self._exists(
            self._build_absolute_path(boundary_name, self.datasets_folder_name)
        )

    def boundary_file_exists(self, boundary_name: str, filename: str):
        """If a given file for a boundary exists"""
        return self._exists(self._build_absolute_path(boundary_name, filename))

    @abstractmethod
    def put_boundary_data(
        self,
        local_source_fpath: str,
        boundary_name: str,
    ):
        """Put a boundary supporting data onto the backend"""

    @abstractmethod
    def count_boundary_data_files(
        self,
        boundary_name: str,
        dataset_name: str,
        version: str,
        datafile_ext: str = ".tif",
    ) -> int:
        """
        Count the number of datafiles for a given boundary folder
        """

    @abstractmethod
    def remove_boundary_data_files(
        self,
        boundary_name: str,
        dataset_name: str,
        version: str,
    ):
        """Remove all datafiles associated with a particular boundary + processing version"""

    @abstractmethod
    def processor_file_exists(
        self, boundary_name: str, dataset_name: str, version: str, filename: str
    ):
        """If a given file for a dataset processor exists"""

    @abstractmethod
    def load_datapackage(self, boundary_name: str) -> dict:
        """Load the datapackage.json file from backend and return"""
