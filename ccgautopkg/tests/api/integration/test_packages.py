"""
Tests for Packages
"""

import os
import sys
import inspect
import unittest

import requests

from tests.helpers import build_route, create_tree, remove_tree

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import PACKAGE_ROUTE, PACKAGES_BASE_ROUTE
from config import INTEGRATION_TEST_ENDPOINT, LOCALFS_STORAGE_BACKEND_ROOT


class TestPackages(unittest.TestCase):

    """
    These tests require API and Celery Worker to be run ning (with redis)
    """

    def assert_package(self, response, expected_boundary_name: str, expected_dataset_names_versions: list):
        """
        Check the package repsonse is valid

        ::param expected_dataset_names_versions list ["natural_earth.version_1", ...]
        """
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['boundary_name'], expected_boundary_name)
        name_versions = []
        for dataset in response.json()["datasets"]:
            for version in dataset['versions']:
                name_versions.append(f'{dataset["name"]}.{version["version"]}')
        self.assertListEqual(
            name_versions,
            expected_dataset_names_versions
        )

    def test_get_all_packages(self):
        """
        Retrieve all packages
        """
        if not LOCALFS_STORAGE_BACKEND_ROOT:
            raise Exception("localfs storage root not set in env")
        create_tree(LOCALFS_STORAGE_BACKEND_ROOT)
        route = build_route(PACKAGES_BASE_ROUTE)
        response = requests.get(route)
        # Ensure we can find at least the fake packages we created
        self.assertIn(
            'zambia',
            [boundary['boundary_name'] for boundary in response.json()]
        )
        self.assertIn(
            'gambia',
            [boundary['boundary_name'] for boundary in response.json()]
        )
        remove_tree(LOCALFS_STORAGE_BACKEND_ROOT)

    def test_get_package_by_name_not_found(self):
        """Attempt to retrieve details of a package which does not exist"""
        route = build_route(PACKAGE_ROUTE.format(boundary_name='noexist'))
        response = requests.get(route)
        self.assertEqual(response.status_code, 404)
        self.assertDictEqual(response.json(), {'detail': 'Package noexist not found'})

    def test_get_package_by_name_no_valid_datasets(self):
        """
        Attempt to Retrieve details of a package by boundary name, 
        where there are no datasets which have applicable processors
        """
        create_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=['gambia'], datasets=['noexist'])
        route = build_route(PACKAGE_ROUTE.format(boundary_name='gambia'))
        response = requests.get(route)
        self.assertEqual(response.status_code, 404)
        self.assertDictEqual(response.json(), {'detail': 'Package gambia has no existing or executing datasets'})
        remove_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=['gambia'])

    def test_get_package_by_name(self):
        """
        Retrieve details of a package by boundary name

        Package is created within the test, but the processor must exist and be valid (natural_earth.version_1)
        """
        create_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=['gambia'], datasets=['natural_earth'])
        route = build_route(PACKAGE_ROUTE.format(boundary_name='gambia'))
        response = requests.get(route)
        self.assert_package(response, "gambia", ["natural_earth.version_1"])
        remove_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=['gambia'])

