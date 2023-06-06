"""
Tests for Probes
"""

import os
import sys
import inspect
import unittest

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import LIVENESS_ROUTE, READINESS_ROUTE
from config import INTEGRATION_TEST_ENDPOINT
from tests.helpers import build_route


class TestProbes(unittest.TestCase):

    """
    These tests require API and Celery Worker to be run ning (with redis)
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
