"""
Tests for Processor Endpoints
"""

import os
import sys
import inspect
import unittest

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import (
    PROCESSORS_BASE_ROUTE,
    PROCESSORS_NAME_ROUTE,
    PROCESSORS_VERSION_ROUTE,
)
from tests.helpers import build_route

EXPECTED_PROCESSOR_VERSION = {
    "name": "test_processor.version_1",
    "description": "A test processor for nightlights",
    "version": "version_1",
    "data_author": "Nightlights Author",
    "data_title": "",
    "data_title_long": "",
    "data_summary": "",
    "data_citation": "",
    "data_license": {
        "name": "CC-BY-4.0",
        "path": "https://creativecommons.org/licenses/by/4.0/",
        "title": "Creative Commons Attribution 4.0",
    },
    "data_origin_url": "http://url",
}


class TestProcessors(unittest.TestCase):

    """
    These tests require API to be running
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_all_processors(self):
        """
        Retrieve all Processors
        """
        route = build_route(PROCESSORS_BASE_ROUTE)
        response = requests.get(route)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.json()) > 0)
        self.assertIn("test_processor", [proc['name'] for proc in response.json()])

    def test_get_processor_name_noexist(self):
        """
        Retrieve a processor by name which does not exist
        """
        route = build_route(PROCESSORS_NAME_ROUTE.format(name="noexist"))
        response = requests.get(route)
        self.assertEqual(response.status_code, 404)

    def test_get_processor_name_version_noexist(self):
        """
        Retrieve a processor version which does not exist
        """
        route = build_route(
            PROCESSORS_VERSION_ROUTE.format(name="test_processor", version="noexist")
        )
        response = requests.get(route)
        self.assertEqual(response.status_code, 404)

    def test_get_processor_by_name(self):
        """
        Retrieve a processor by name
        """
        route = build_route(PROCESSORS_NAME_ROUTE.format(name="test_processor"))
        response = requests.get(route)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["name"], "test_processor")
        self.assertEqual(len(response.json()["versions"]), 1)
        self.assertDictEqual(response.json()["versions"][0], EXPECTED_PROCESSOR_VERSION)

    def test_get_processor_version(self):
        """
        Retrieve a processor version
        """
        route = build_route(
            PROCESSORS_VERSION_ROUTE.format(
                name="test_processor", version=EXPECTED_PROCESSOR_VERSION["version"]
            )
        )
        response = requests.get(route)
        self.assertDictEqual(response.json(), EXPECTED_PROCESSOR_VERSION)
