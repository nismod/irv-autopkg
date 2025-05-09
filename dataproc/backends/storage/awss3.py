"""
AWS S3 Filesystem Backend
"""

import os
from typing import List
import json
import warnings

from pyarrow import fs

from dataproc.backends.base import StorageBackend


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
        dict.__init__(self)
        self.bucket = bucket
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_region = s3_region
        self._check_env()

    def _check_env(self) -> bool:
        """
        Check the env required for S3 appears to be valid
        """
        if not all([self.s3_access_key, self.s3_secret_key]):
            warnings.warn(
                "AWSS3StorageBackend - s3_access_key and s3_secret_key required for S3 initialisation"
            )

    def _build_absolute_path(self, *args) -> str:
        """
        Build an absolute path from a relative path, by pre-pending the configured top level directory
        """
        return os.path.join(self.bucket, *args)

    def _list_directories(self, absolute_s3_path: str, recursive=False) -> List[str]:
        """
        List the paths to directories in a given absolute_s3_path path (i.e. includes the bucket name)

        ::kwarg recursive optionally recurse down the tree
        """
        with S3Manager(
            self.s3_access_key, self.s3_secret_key, region=self.s3_region
        ) as s3_fs:
            contents = s3_fs.get_file_info(
                fs.FileSelector(absolute_s3_path, recursive=recursive)
            )
            return [
                os.path.basename(item.path)
                for item in contents
                if item.type == fs.FileType.Directory
            ]

    def packages(self) -> List[str]:
        """List of Packages that currently exist under the top-level storage backend"""

        tree = {}
        for package in self._list_directories(self._build_absolute_path("")):
            # First level packages
            tree[package] = {}
        return list(tree.keys())

    def load_datapackage(self, boundary_name: str) -> dict:
        """Load the datapackage.json file from backend and return"""
        datapackage_fpath = self._build_absolute_path(boundary_name, "datapackage.json")
        with S3Manager(*self._parse_env(), region=self.s3_region) as s3_fs:
            with s3_fs.open_input_stream(datapackage_fpath) as stream:
                datapackage = json.loads(stream.readall().decode())
                return datapackage
