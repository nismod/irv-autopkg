"""
Tests for Packages
"""

import os
import sys
import inspect
import unittest

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import PACKAGE_ROUTE, PACKAGES_BASE_ROUTE
from tests.helpers import (
    build_route,
    create_tree,
    remove_tree,
    assert_datapackage_resource,
    create_tree_awss3,
    remove_tree_awss3,
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
)
from dataproc.backends.storage.awss3 import AWSS3StorageBackend, S3Manager
from config import (
    STORAGE_BACKEND,
    S3_BUCKET,
    S3_ACCESS_KEY_ENV,
    S3_SECRET_KEY_ENV,
)


class TestPackages(unittest.TestCase):

    """
    These tests require API and Celery Worker to be run ning (with redis)
    """

    def setUp(self):
        self.backend = AWSS3StorageBackend(
            S3_BUCKET, S3_ACCESS_KEY_ENV, S3_SECRET_KEY_ENV
        )

    def assert_package(
        self,
        response,
        expected_boundary_name: str,
        expected_dataset_names_versions: list,
    ):
        """
        Check the package repsonse is valid

        ::param expected_dataset_names_versions list ["natural_earth_raster.version_1", ...]
        """
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["boundary_name"], expected_boundary_name)
        name_versions = []
        # Check the processors
        for dataset in response.json()["processors"]:
            for version in dataset["versions"]:
                name_versions.append(f'{dataset["name"]}.{version["version"]}')
        self.assertListEqual(name_versions, expected_dataset_names_versions)
        # Ensure we have a nested datapackage
        self.assertIn("datapackage", response.json().keys())
        for dp_resource in response.json()["datapackage"]["resources"]:
            assert_datapackage_resource(dp_resource)

    def test_get_all_packages(self):
        """
        Retrieve all packages
        """
        if STORAGE_BACKEND == "localfs":
            create_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR)
        elif STORAGE_BACKEND == "awss3":
            with S3Manager(
                *self.backend._parse_env(), region=self.backend.s3_region
            ) as s3_fs:
                create_tree_awss3(
                    s3_fs,
                    S3_BUCKET,
                )
        route = build_route(PACKAGES_BASE_ROUTE)
        response = requests.get(route)
        # Ensure we can find at least the fake packages we created
        self.assertIn(
            "zambia", [boundary["boundary_name"] for boundary in response.json()]
        )
        self.assertIn(
            "gambia", [boundary["boundary_name"] for boundary in response.json()]
        )
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR)

    def test_get_package_by_name_not_found(self):
        """Attempt to retrieve details of a package which does not exist"""
        route = build_route(PACKAGE_ROUTE.format(boundary_name="noexist"))
        response = requests.get(route)
        self.assertEqual(response.status_code, 404)
        self.assertDictEqual(response.json(), {"detail": "Package noexist not found"})

    def test_get_package_by_name_no_valid_datasets(self):
        """
        Attempt to Retrieve details of a package by boundary name,
        where there are no datasets which have applicable processors
        """
        if STORAGE_BACKEND == "localfs":
            create_tree(
                LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"], datasets=["noexist"]
            )
        elif STORAGE_BACKEND == "awss3":
            with S3Manager(
                *self.backend._parse_env(), region=self.backend.s3_region
            ) as s3_fs:
                create_tree_awss3(
                    s3_fs, S3_BUCKET, packages=["gambia"], datasets=["noexist"]
                )
        route = build_route(PACKAGE_ROUTE.format(boundary_name="gambia"))
        response = requests.get(route)
        self.assertEqual(response.status_code, 404)
        self.assertDictEqual(
            response.json(),
            {"detail": "Package gambia has no existing or executing datasets"},
        )
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])

    def test_get_package_by_name(self):
        """
        Retrieve details of a package by boundary name

        Package is created within the test, but the processor must exist and be valid (natural_earth_raster.version_1)
        """
        if STORAGE_BACKEND == "localfs":
            create_tree(
                LOCAL_FS_PACKAGE_DATA_TOP_DIR,
                packages=["gambia"],
                datasets=["natural_earth_raster"],
            )
        elif STORAGE_BACKEND == "awss3":
            with S3Manager(
                *self.backend._parse_env(), region=self.backend.s3_region
            ) as s3_fs:
                create_tree_awss3(
                    s3_fs,
                    S3_BUCKET,
                    packages=["gambia"],
                    datasets=["natural_earth_raster"],
                )
        route = build_route(PACKAGE_ROUTE.format(boundary_name="gambia"))
        response = requests.get(route)
        self.assert_package(response, "gambia", ["natural_earth_raster.version_1"])
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])
