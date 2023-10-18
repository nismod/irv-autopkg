"""
AWS S3 Filesystem Backend
"""

import os
from typing import Dict, List, Tuple
import json
from datetime import datetime
import warnings

from pyarrow import fs

from dataproc.exceptions import (
    FolderCreationException,
    FileCreationException,
    PackageNotFoundException,
    DatasetNotFoundException,
    S3Exception,
)
from dataproc import DataPackageResource
from dataproc import helpers
from config import PACKAGES_HOST_URL
from ..base import StorageBackend


class S3Manager:
    """
    S3 FS Context Manager

    ::arg access_key str
    ::arg secret_key str
    ::kwarg region str

    """

    def __init__(self, *args, region="eu-west-2"):
        self.access_key = args[0]
        self.secret_key = args[1]
        self.region = region
        self.s3_fs = None

    def __enter__(self) -> fs.S3FileSystem:
        self.s3_fs = fs.S3FileSystem(
            region=self.region, access_key=self.access_key, secret_key=self.secret_key
        )
        return self.s3_fs

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.s3_fs:
            del self.s3_fs


class AWSS3StorageBackend(StorageBackend):
    """Backend for AWS S3 filesystem"""

    def __init__(
        self,
        bucket: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_region="eu-west-2",
    ) -> None:
        """

        ::param bucket str S3 bucket under-which packages are stored
        ::param s3_access_key str S3 access key
        ::param s3_secret_key str S3 secret key
        """
        self.bucket = bucket
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_region = s3_region
        _ = self._check_env()

    def _parse_env(self) -> Tuple[str, str]:
        """
        Parse the S3 secrets from env

        ::returns Tuple[str, str] access_key, secret_key
        """
        return self.s3_access_key, self.s3_secret_key

    def _check_env(self) -> bool:
        """
        Check the env required for S3 appears to be valid
        """
        if not all([self.s3_access_key, self.s3_secret_key]):
            warnings.warn(
                "AWSS3StorageBackend - s3_access_key and s3_secret_key required for S3 initialisation"
            )
            return False
        return True

    def _build_absolute_path(self, *args) -> str:
        """
        Build an absolute path from a relative path, by pre-pending the configured top level directory
        """
        return os.path.join(self.bucket, *args)

    def _build_uri(self, absolute_fpath: str) -> str:
        """Build the internet-accessible URI from a given s3 fpath"""
        return absolute_fpath.replace(self.bucket, PACKAGES_HOST_URL)

    def _remove_bucket_from_s3path(self, s3_path: str) -> str:
        """
        Remove the bucket name from a given as3_path
        """
        return s3_path.replace(self.bucket + "/", "")

    def _list_directories(self, absolute_s3_path: str, recursive=False) -> List[str]:
        """
        List the paths to directories in a given absolute_s3_path path (i.e. includes the bucket name)

        ::kwarg recursive optionally recurse down the tree
        """
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            contents = s3_fs.get_file_info(
                fs.FileSelector(absolute_s3_path, recursive=recursive)
            )
            return [
                os.path.basename(item.path)
                for item in contents
                if item.type == fs.FileType.Directory
            ]

    def _exists(self, absolute_s3_path: str) -> bool:
        """
        Check if an object at the given path exists
        """
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            chk = s3_fs.get_file_info(absolute_s3_path)
            return chk.type != fs.FileType.NotFound

    def tree(self, summary: bool = False) -> Dict:
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
        tree: Dict[str, Dict[str, List[str]]] = {}
        if summary is True:
            for package in self._list_directories(self._build_absolute_path("")):
                # First level packages
                tree[package] = {}
            return tree
        # Descend into datasets and versions
        # Single call to S3
        for package, _ in tree.items():
            for dataset in self._list_directories(
                self._build_absolute_path(package, self.datasets_folder_name)
            ):
                tree[package][dataset] = []
        # Descend into versions
        for package, _ in tree.items():
            for dataset, _ in tree[package].items():
                for version in self._list_directories(
                    self._build_absolute_path(
                        package, self.datasets_folder_name, dataset
                    )
                ):
                    tree[package][dataset].append(version)
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
        try:
            return self._list_directories(
                self._build_absolute_path(package, self.datasets_folder_name)
            )
        except:
            # The package does not exist
            raise PackageNotFoundException(f"{package}")

    def dataset_versions(self, package: str, dataset: str) -> List[str]:
        """
        List of versions that currently exist for a given Dataset

        ::param package str The name of the package
            (which maps directly to a Boundary name)

        ::param dataset str the name of the dataset for which to retrieve versions
        """
        try:
            return self._list_directories(
                self._build_absolute_path(package, self.datasets_folder_name, dataset)
            )
        except:
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
        # If no exist - stream new to path
        if not self._exists(dest_abs_path):
            log = {datetime.utcnow().isoformat(): processing_log}
            with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
                with s3_fs.open_output_stream(dest_abs_path) as stream:
                    stream.write(json.dumps(log).encode())
        else:
            # If exist - fetch, update and upload (overwrite)
            with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
                with s3_fs.open_input_stream(dest_abs_path) as stream:
                    log = json.loads(stream.readall().decode())
                    log[datetime.utcnow().isoformat()] = processing_log
                with s3_fs.open_output_stream(dest_abs_path) as stream:
                    stream.write(json.dumps(log).encode())
        return True

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

    def create_boundary_folder(self, boundary_name: str):
        """
        Create a boundary folder
        """
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            s3_fs.create_dir(self._build_absolute_path(boundary_name))
        if not self.boundary_folder_exists(boundary_name):
            raise FolderCreationException(
                f"boundary folder path {boundary_name} not found"
            )

    def create_boundary_data_folder(self, boundary_name: str):
        """
        Create a boundary data folder
        """
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            s3_fs.create_dir(
                self._build_absolute_path(boundary_name, self.datasets_folder_name)
            )
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
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            fs.copy_files(
                local_source_fpath,
                dest_abs_path,
                source_filesystem=fs.LocalFileSystem(),
                destination_filesystem=s3_fs,
            )
        if not self._exists(dest_abs_path):
            raise FileCreationException(
                f"destination file path {dest_abs_path} not found after creation attempt"
            )

    def processor_dataset_exists(
        self, boundary_name: str, processor_dataset: str, version: str
    ) -> bool:
        """
        Test if a given dataset folder exists within the given boundary and dataset version
        """
        return self._exists(
            self._build_absolute_path(
                boundary_name,
                self.datasets_folder_name,
                processor_dataset,
                version,
            )
        )

    def processor_file_exists(
        self, boundary_name: str, dataset_name: str, version: str, filename: str
    ):
        """If a given file for a dataset processor exists"""
        return self._exists(
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
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            # Creates directories as necessary
            fs.copy_files(
                local_source_fpath,
                dest_abs_path,
                source_filesystem=fs.LocalFileSystem(),
                destination_filesystem=s3_fs,
            )
        if not self._exists(dest_abs_path):
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
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            # Creates directories as necessary
            fs.copy_files(
                local_source_fpath,
                dest_abs_path,
                source_filesystem=fs.LocalFileSystem(),
                destination_filesystem=s3_fs,
            )
        if not self._exists(dest_abs_path):
            raise FileCreationException(
                f"destination file path {dest_abs_path} not found after creation attempt"
            )
        if remove_local_source is True:
            os.remove(local_source_fpath)
        return self._build_uri(dest_abs_path)

    def count_file_types_in_folder(self, folder_path: str, file_type="tif") -> int:
        """
        Count the number of files of a type in a folder
        """
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            contents = s3_fs.get_file_info(
                fs.FileSelector(folder_path, recursive=False)
            )
            return len(
                [
                    item
                    for item in contents
                    if item.type == fs.FileType.File and item.extension == file_type
                ]
            )

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
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            contents = s3_fs.get_file_info(fs.FileSelector(folder, recursive=False))
            return len(
                [
                    item
                    for item in contents
                    if item.type == fs.FileType.File
                    and item.extension == datafile_ext.replace(".", "")
                ]
            )

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
        if not self._exists(folder):
            raise FileNotFoundError()
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            s3_fs.delete_dir_contents(folder, missing_dir_ok=True)

    def update_datapackage(self, boundary_name: str, dp_resource: DataPackageResource):
        """
        Update a packages datapackage.json file with details of a given dataset.

        __NOTE__: Assumes the Boundary processor has already run and datapackage exists (even as just template)
        """
        datapackage_fpath = self._build_absolute_path(boundary_name, "datapackage.json")
        if not self._exists(datapackage_fpath):
            raise S3Exception(f"datapackage does not exist: {datapackage_fpath}")
        # Fetch, append, write
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            with s3_fs.open_input_stream(datapackage_fpath) as stream:
                datapackage = json.loads(stream.readall().decode())
                datapackage = helpers.add_dataset_to_datapackage(
                    dp_resource,
                    datapackage,
                )
            with s3_fs.open_output_stream(datapackage_fpath) as stream:
                stream.write(json.dumps(datapackage).encode())

    def load_datapackage(self, boundary_name: str) -> Dict:
        """Load the datapackage.json file from backend and return"""
        datapackage_fpath = self._build_absolute_path(boundary_name, "datapackage.json")
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            with s3_fs.open_input_stream(datapackage_fpath) as stream:
                datapackage = json.loads(stream.readall().decode())
                return datapackage
