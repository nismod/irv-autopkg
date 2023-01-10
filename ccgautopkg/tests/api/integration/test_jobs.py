"""
Tests for Processing Jobs
"""

import os
import sys
import inspect
import unittest
from uuid import uuid4

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE
from config import INTEGRATION_TEST_ENDPOINT
from tests.helpers import build_route


class TestProcessingJobs(unittest.TestCase):

    """
    These tests require API and Celery Worker to be run ning (with redis)
    """

    def test_get_job_no_exist(self):
        expected_code = 404
        route = build_route(JOB_STATUS_ROUTE.format(job_id=str(uuid4())))
        response = requests.get(route)
        self.assertEqual(response.status_code, expected_code)

    def test_submit_job(self):
        TODO

    def test_submit_job_no_such_boundary(self):
        """Submission of a job against a boundary that doesnt exist"""
        TODO

    def test_submit_job_no_such_processor(self):
        """Submission of a job against a processor that doesnt exist"""
        TODO

    def test_submit_job_duplicate_processor(self):
        """Submission of a job with duplicate processors in the request"""
        TODO

    def test_submit_job_already_processing(self):
        """Submission of a second job containing the same dataset while one is already executing"""
        TODO

    def test_submit_job_already_exists(self):
        """Submission of a job for a dataset and version that already exist"""
        TODO