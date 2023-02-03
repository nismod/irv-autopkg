"""
Tests for Boundaries
"""

import os
import sys
import inspect
import unittest

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
api_tests_dir = os.path.dirname(current_dir)
tests_data_dir = os.path.join(os.path.dirname(api_tests_dir), "data")
sys.path.insert(0, api_tests_dir)

from api.routes import BOUNDARIES_BASE_ROUTE, BOUNDARY_ROUTE, BOUNDARY_SEARCH_ROUTE
from tests.helpers import wipe_db, build_route
from tests.data.load_boundaries import load_boundaries

TEST_BOUNDARIES_FPATH = os.path.join(tests_data_dir, "africa_europe.geojson")
TOTAL_BOUNDARIES = 90


class TestBoundaries(unittest.TestCase):

    """"""

    def setUp(self) -> None:
        # Wipe the DB
        wipe_db()
        # Load Boundaries
        load_boundaries(TEST_BOUNDARIES_FPATH)

    def tearDown(self) -> None:
        # Wipe the DB
        # wipe_db()
        pass

    def assert_geometry(self, geometry: dict):
        """Check a GeoJSON Geom"""
        self.assertIn("coordinates", geometry.keys())
        self.assertEqual(geometry["type"], "MultiPolygon")

    def assert_boundary_detail(self, response, expected_count=None):
        """
        Assert the given response contains only boundary details (one or more)
            Optionally assert an expected count
        """
        self.assertEqual(response.status_code, 200)
        if isinstance(response.json(), list):
            for item in response.json():
                self.assert_geometry(item["geometry"])
        else:
            self.assert_geometry(response.json()["geometry"])
        if expected_count:
            self.assertEqual(len(response.json()), expected_count)

    def assert_boundary_summary(self, response, expected_count=None):
        """
        Assert the given response contains only boundary summaries (one or more)
        """
        self.assertEqual(response.status_code, 200)
        if isinstance(response.json(), list):
            for item in response.json():
                self.assertNotIn("geometry", item.keys())
        else:
            self.assertNotIn("geometry", item.keys())
        if expected_count:
            self.assertEqual(len(response.json()), expected_count)

    def test_get_all_boundary_summaries(self):
        """
        Retrieve all boundary summary info
        """
        route = build_route(BOUNDARIES_BASE_ROUTE)
        response = requests.get(route)
        self.assert_boundary_summary(response, expected_count=TOTAL_BOUNDARIES)

    def test_get_boundary_by_name(self):
        """
        Retrieve a single boundary explicitly by name
        """
        expected_name = "ssudan"
        route = build_route(BOUNDARY_ROUTE.format(name=expected_name))
        response = requests.get(route)
        self.assert_boundary_detail(response)
        self.assertEqual(response.json()["name"], expected_name)

    def test_get_boundary_by_name_noexist(self):
        """
        Retrieve a single boundary explicitly by name
        """
        expected_name = "lkjhasdklfj"
        route = build_route(BOUNDARY_ROUTE.format(name=expected_name))
        response = requests.get(route)
        self.assertEqual(response.status_code, 404)

    def test_search_boundary_by_name(self):
        """
        Retrieve boundaries by searching for a name
        """
        search_name = 'gh'
        expected_names = ['ghana', 'guinea']
        route = build_route(f"{BOUNDARY_SEARCH_ROUTE}?name={search_name}")
        response = requests.get(route)
        self.assert_boundary_summary(response, expected_count=len(expected_names))
        self.assertCountEqual(
            [item["name"] for item in response.json()], expected_names)

    def test_search_boundary_by_name_nothing_found(self):
        """
        Retrieve boundaries by searching for a name that finds nothing
        """
        search_name = 'kjhasdlfkhjasdf'
        expected_names = []
        route = build_route(f"{BOUNDARY_SEARCH_ROUTE}?name={search_name}")
        response = requests.get(route)
        self.assert_boundary_summary(response, expected_count=len(expected_names))
        self.assertCountEqual(
            [item["name"] for item in response.json()], expected_names)

    def test_search_boundary_by_coords(self):
        search_latitude = 28.2
        search_longitude = 3.2
        expected_names = ['algeria']
        route = build_route(f"{BOUNDARY_SEARCH_ROUTE}?latitude={search_latitude}&longitude={search_longitude}")
        response = requests.get(route)
        self.assert_boundary_summary(response, expected_count=len(expected_names))
        self.assertCountEqual(
            [item["name"] for item in response.json()], expected_names)

    def test_search_boundary_by_coords_nothing_found(self):
        search_latitude = 128.2
        search_longitude = 113.2
        expected_names = []
        route = build_route(f"{BOUNDARY_SEARCH_ROUTE}?latitude={search_latitude}&longitude={search_longitude}")
        response = requests.get(route)
        self.assert_boundary_summary(response, expected_count=len(expected_names))
        self.assertCountEqual(
            [item["name"] for item in response.json()], expected_names)

