"""
Tests for Probes
"""

import unittest

import requests

from api.routes import LIVENESS_ROUTE, READINESS_ROUTE
from tests.helpers import build_route


class TestProbes(unittest.TestCase):
    """
    These tests require the API to be running
    """

    def build_probes_route(self, probe_type):
        if probe_type == "liveness":
            _route = LIVENESS_ROUTE
        else:
            _route = READINESS_ROUTE
        return build_route(_route)

    def test_liveness(self):
        expected_code = 200
        route = self.build_probes_route("liveness")
        response = requests.get(route)
        self.assertEqual(response.status_code, expected_code)

    def test_readinessness(self):
        expected_code = 200
        route = self.build_probes_route("readiness")
        response = requests.get(route)
        self.assertEqual(response.status_code, expected_code)
