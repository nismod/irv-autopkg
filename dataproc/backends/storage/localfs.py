"""
Local Filesystem Backend
"""

import os
import shutil
from typing import List
import json
from datetime import datetime

from dataproc.exceptions import (
    FolderCreationException,
    FileCreationException,
    PackageNotFoundException,
    DatasetNotFoundException,
    FolderNotFoundException,
)
from dataproc import DataPackageResource
from dataproc import helpers
from ..base import StorageBackend
from config import PACKAGES_HOST_URL


class LocalFSStorageBackend(StorageBackend):
    """Backend for local filesystem"""

    def __init__(self, top_level_folder_path: str) -> None:
        dict.__init__(self)
        self.top_level_folder_path = top_level_folder_path

    def _build_absolute_path(self, *args) -> str:
        """
        Build an absolute path from a relative path, by pre-pending the configured top level directory
        """
        return os.path.join(self.top_level_folder_path, *args)

    def _build_uri(self, absolute_fpath: str) -> str:
        """Build the internet-accessible URI from a given localFS absolute fpath"""
        return absolute_fpath.replace(self.top_level_folder_path, PACKAGES_HOST_URL)

    def tree(self, summary: bool = False) -> dict:
        """
        Generate a source-of-truth tree for the
        FS showing Packages and Processors
        (with their versions)
        {
            "package_name":{
                "processor_name": ["version_id", ...]
            },
            "package_name":{
                "processor_name": ["version_id", ...]
            }
        }
        ::kwarg summary bool Return only boundary names (packages), not included dataset_versions
        """
        tree = {}
        for _, dirs, _ in os.walk(os.path.join(self.top_level_folder_path)):
            # First level packages
            for package in dirs:
                tree[package] = {}
            # Dont recurse further
            break
        if summary is True:
            return tree
        # Descend into datasets
        for package, _ in tree.items():
            for _, dataset_dirs, _ in os.walk(
                os.path.join(
                    self.top_level_folder_path,
                    package,
                    self.datasets_folder_name,
                )
            ):
                for dataset in dataset_dirs:
                    tree[package][dataset] = []
                # Dont recurse further
                break
        # Descend into versions
        for package, _ in tree.items():
            for dataset, _ in tree[package].items():
                for _, version_dirs, _ in os.walk(
                    os.path.join(
                        self.top_level_folder_path,
                        package,
                        self.datasets_folder_name,
                        dataset,
                    )
                ):
                    for version in version_dirs:
                        tree[package][dataset].append(version)
                    # Dont recurse further
                    break
        return tree

    def packages(self, summary: bool = False) -> List[str]:
        """List of Packages that currently exist under the top-level storage backend"""
        tree = self.tree(summary=summary)
        return list(tree.keys())

    def package_datasets(self, package: str) -> List[str]:
        """
        List of Datasets that currently exist for a given Package

        ::param package str The name of the package
            (which maps directly to a Boundary name)
        """
        tree = self.tree()
        try:
            return list(tree[package].keys())
        except KeyError:
            # The package does not exist
            raise PackageNotFoundException(f"{package}")

    def dataset_versions(self, package: str, dataset: str) -> List[str]:
        """
        List of versions that currently exist for a given Dataset

        ::param package str The name of the package
            (which maps directly to a Boundary name)

        ::param dataset str the name of the dataset for which to retrieve versions
        """
        tree = self.tree()
        try:
            return tree[package][dataset]
        except KeyError:
            # The dataset does not exist
            raise DatasetNotFoundException(f"{dataset}")

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
        dest_abs_path = self._build_absolute_path(boundary_name, filename)
        if not os.path.exists(dest_abs_path):
            with open(dest_abs_path, "w") as fptr:
                log = {datetime.utcnow().isoformat(): processing_log}
                json.dump(log, fptr)
        else:
            with open(dest_abs_path, "r") as fptr:
                log = json.load(fptr)
            log[datetime.utcnow().isoformat()] = processing_log
            with open(dest_abs_path, "w") as fptr:
                json.dump(log, fptr)
        return os.path.exists(dest_abs_path)

    def boundary_folder_exists(self, boundary_name: str):
        """If a given boundary folder exists"""
        return os.path.exists(self._build_absolute_path(boundary_name))

    def boundary_data_folder_exists(self, boundary_name: str):
        """If a given boundary data folder exists"""
        return os.path.exists(
            self._build_absolute_path(boundary_name, self.datasets_folder_name)
        )

    def boundary_file_exists(self, boundary_name: str, filename: str):
        """If a given file for a boundary exists"""
        return os.path.exists(self._build_absolute_path(boundary_name, filename))

    def create_boundary_folder(self, boundary_name: str):
        """
        Create a boundary folder
        """
        full_path = self._build_absolute_path(boundary_name)
        os.mkdir(full_path)
        if not self.boundary_folder_exists(boundary_name):
            raise FolderCreationException(
                f"boundary folder path {boundary_name} not found"
            )

    def create_boundary_data_folder(self, boundary_name: str):
        """
        Create a boundary data folder
        """
        full_path = self._build_absolute_path(boundary_name, self.datasets_folder_name)
        os.mkdir(full_path)
        if not self.boundary_data_folder_exists(boundary_name):
            raise FolderCreationException(
                f"boundary data-folder path {boundary_name} not found"
            )

    def put_boundary_data(
        self,
        local_source_fpath: str,
        boundary_name: str,
    ):
        """Put a boundary supporting data onto the backend"""
        filename = os.path.basename(local_source_fpath)
        dest_abs_path = self._build_absolute_path(boundary_name, filename)
        _ = shutil.copy(local_source_fpath, dest_abs_path)
        if not os.path.exists(dest_abs_path):
            raise FileCreationException(
                f"destination file path {dest_abs_path} not found after creation attempt"
            )

    def processor_dataset_exists(
        self, boundary_name: str, processor_dataset: str, version: str
    ) -> bool:
        """
        Test if a given dataset folder exists within the given boundary and dataset version
        """
        abs_path = self._build_absolute_path(
            boundary_name,
            self.datasets_folder_name,
            processor_dataset,
            version,
        )
        return os.path.exists(abs_path)

    def processor_file_exists(
        self, boundary_name: str, dataset_name: str, version: str, filename: str
    ):
        """If a given file for a dataset processor exists"""
        return os.path.exists(
            self._build_absolute_path(
                boundary_name,
                self.datasets_folder_name,
                dataset_name,
                version,
                self.dataset_data_folder_name,
                filename,
            )
        )

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
        filename = os.path.basename(local_source_fpath)
        dest_abs_path = self._build_absolute_path(
            boundary_name,
            self.datasets_folder_name,
            dataset_name,
            version,
            self.dataset_data_folder_name,
            filename,
        )
        # Create the output dirs
        os.makedirs(os.path.dirname(dest_abs_path), exist_ok=True)
        _ = shutil.copy(local_source_fpath, dest_abs_path)
        if not os.path.exists(dest_abs_path):
            raise FileCreationException(
                f"destination file path {dest_abs_path} not found after creation attempt"
            )
        if remove_local_source is True:
            os.remove(local_source_fpath)
        return self._build_uri(dest_abs_path)

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
        filename = os.path.basename(local_source_fpath)
        dest_abs_path = self._build_absolute_path(
            boundary_name,
            self.datasets_folder_name,
            dataset_name,
            version,
            filename,
        )
        # Create the output dirs
        os.makedirs(os.path.dirname(dest_abs_path), exist_ok=True)
        _ = shutil.copy(local_source_fpath, dest_abs_path)
        if not os.path.exists(dest_abs_path):
            raise FileCreationException(
                f"destination file path {dest_abs_path} not found after creation attempt"
            )
        if remove_local_source is True:
            os.remove(local_source_fpath)
        return self._build_uri(dest_abs_path)

    @staticmethod
    def count_file_types_in_folder(folder_path: str, file_type="tif") -> int:
        """
        Count the number of files of a type in a folder
        """
        count = 0
        for dir_info in os.scandir(folder_path):
            if os.path.splitext(dir_info.name)[1] == file_type:
                count += 1
        return count

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
        folder = self._build_absolute_path(
            boundary_name,
            self.datasets_folder_name,
            dataset_name,
            version,
            self.dataset_data_folder_name,
        )
        if not os.path.exists(folder):
            raise FileNotFoundError()
        return self.count_file_types_in_folder(folder, datafile_ext)

    def remove_boundary_data_files(
        self,
        boundary_name: str,
        dataset_name: str,
        version: str,
    ):
        """Remove all datafiles associated with a particular boundary + processing version"""
        folder = self._build_absolute_path(
            boundary_name,
            self.datasets_folder_name,
            dataset_name,
            version,
            self.dataset_data_folder_name,
        )
        if not os.path.exists(folder):
            raise FileNotFoundError()
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as err:
                print(f"Failed to delete {file_path}. Reason: {err}")

    def update_datapackage(self, boundary_name: str, dp_resource: DataPackageResource):
        """
        Update a packages datapackage.json file with details of a given dataset.

        __NOTE__: Assumes the Boundary processor has already run and datapackage exists (even as just template)
        """
        # Load existing datapackage
        datapackage_fpath = self._build_absolute_path(boundary_name, "datapackage.json")
        with open(datapackage_fpath, "r") as fptr:
            datapackage = json.load(fptr)

        datapackage = helpers.add_dataset_to_datapackage(
            dp_resource,
            datapackage,
        )

        with open(datapackage_fpath, "w") as fptr:
            json.dump(datapackage, fptr)

    def load_datapackage(self, boundary_name: str) -> dict:
        """Load the datapackage.json file from backend and return"""
        datapackage_fpath = self._build_absolute_path(boundary_name, "datapackage.json")
        with open(datapackage_fpath, "r") as fptr:
            datapackage = json.load(fptr)
            return datapackage
