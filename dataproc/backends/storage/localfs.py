"""
Local Filesystem Backend
"""

import os
from typing import List
import json

from dataproc.exceptions import (
    PackageNotFoundException,
    DatasetNotFoundException,
)
from dataproc import DataPackageResource
from dataproc.backends.base import StorageBackend
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

    def load_datapackage(self, boundary_name: str) -> dict:
        """Load the datapackage.json file from backend and return"""
        datapackage_fpath = self._build_absolute_path(boundary_name, "datapackage.json")
        with open(datapackage_fpath, "r") as fptr:
            datapackage = json.load(fptr)
            return datapackage
