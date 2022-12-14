"""
Tests for Processing Jobs
"""

import os
import sys
import inspect
import unittest

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE
from api.config import INTEGRATION_TEST_ENDPOINT


class TestProcessingJobs(unittest.TestCase):

    """"""

    def test_test(self):
        self.assertEqual(1, 0)
