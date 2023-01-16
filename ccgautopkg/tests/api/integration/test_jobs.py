"""
Tests for Processing Jobs
"""

import os
import sys
import inspect
import unittest
from uuid import uuid4
from time import time, sleep

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE
from config import INTEGRATION_TEST_ENDPOINT, LOCALFS_STORAGE_BACKEND_ROOT
from tests.helpers import build_route, remove_tree, assert_package

JOB_SUBMIT_DATA_BOUNDARY_NOEXIST = {
    "boundary_name": "noexist",
    "processors": ["test_natural_earth_raster.version_1"],
}

JOB_SUBMIT_DATA_PROC_NOEXIST = {
    "boundary_name": "gambia",
    "processors": ["noexist.version_1"],
}

JOB_SUBMIT_DATA_PROC_DUP = {
    "boundary_name": "gambia",
    "processors": [
        "test_natural_earth_raster.version_1",
        "test_natural_earth_raster.version_1",
    ],
}

JOB_SUBMIT_DATA_GAMBIA_TEST_PROC = {
    "boundary_name": "gambia",
    "processors": ["test_processor.version_1"],
}  # Awaits 5 secs


class TestProcessingJobs(unittest.TestCase):

    """
    These tests require API and Celery Worker to be running (with redis)
    """

    def setUp(self):
        self.max_job_await = 6  # secs

    def test_get_job_no_exist(self):
        """"""
        # Note - non existant tasks that are assumed as pending (i.e. a random UUID
        expected_code = 200
        expected_status = 'PENDING'
        noexist_job = str(uuid4())
        route = build_route(JOB_STATUS_ROUTE.format(job_id=noexist_job))
        response = requests.get(route)
        self.assertEqual(response.status_code, expected_code)
        self.assertEqual(response.json()['job_status'], expected_status)

    def test_submit_job_no_such_boundary(self):
        """Submission of a job against a boundary that doesnt exist"""
        expected_code = 400
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_BOUNDARY_NOEXIST)
        self.assertEqual(response.status_code, expected_code)
        self.assertDictEqual(response.json(), {
            "detail": "Requested boundary noexist could not be found"
        })

    def test_submit_job_no_such_processor(self):
        """Submission of a job against a processor that doesnt exist"""
        expected_code = 400
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_PROC_NOEXIST)
        self.assertEqual(response.status_code, expected_code)
        self.assertDictEqual(response.json(), {
            "detail": f"Invalid processor.version: {JOB_SUBMIT_DATA_PROC_NOEXIST['processors'][0]}"
        })

    def test_submit_job_duplicate_processor(self):
        """Submission of a job with duplicate processors in the request"""
        expected_code = 422
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_PROC_DUP)
        self.assertEqual(response.status_code, expected_code)
        self.assertEqual(response.json()['detail'][0]['msg'], "duplicate processors not allowed")

    def test_submit_job(self):
        """Simple submission and await completion of a job"""
        # Ensure the package tree is clean
        remove_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=['gambia'])
        expected_code = 202
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_GAMBIA_TEST_PROC)
        self.assertEqual(response.status_code, expected_code)
        self.assertIn('job_id', response.json().keys())
        job_id = response.json()['job_id']
        # Await job completion
        start = time()
        while (time() < start + self.max_job_await):
            route = build_route(JOB_STATUS_ROUTE.format(job_id=job_id))
            response = requests.get(route)
            self.assertEqual(response.json()['job_id'], job_id)
            if not response.json()['job_status'] == 'PENDING':
                break
            sleep(0.2)
        self.assertEqual(response.json()['job_status'], "SUCCESS")
        # Assert the package integrity, including submitted processor
        assert_package(
            LOCALFS_STORAGE_BACKEND_ROOT,
            'gambia',
            JOB_SUBMIT_DATA_GAMBIA_TEST_PROC['processors']
        )
        remove_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=['gambia'])

    def test_submit_job_already_processing(self):
        """
        Submission of a second job containing
            the same boundary and processor while one is already executing
        """
        max_wait = 10  # secs
        dup_processors_to_submit = 2
        expected_responses = [202, 202]
        remove_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=["gambia"])
        route = build_route(JOBS_BASE_ROUTE)
        responses = []
        for _ in range(dup_processors_to_submit):
            response = requests.post(route, json=JOB_SUBMIT_DATA_GAMBIA_TEST_PROC)
            responses.append(response)
        self.assertListEqual(
            [i.status_code for i in responses],
            # Both jobs have been accepted
            expected_responses,
        )
        submitted_ids = [data.json()["job_id"] for data in responses]
        # Wait for both processors to finish
        completed = [False for i in expected_responses]
        statuses = ["NULL", "NULL"]
        results = [{}, {}]
        start = time()
        while not all(completed) or (time() - start) > max_wait:
            for idx, job_id in enumerate(submitted_ids):
                route = build_route(JOB_STATUS_ROUTE.format(job_id=job_id))
                response = requests.get(route)
                completed[idx] = (
                    True
                    if response.json()["job_status"] in ["SUCCESS", "FAILED"]
                    else False
                )
                results[idx] = response.json()["job_result"]
                statuses[idx] = response.json()["job_status"]
            sleep(0.5)
        # Both jobs completed successfully
        self.assertEqual(statuses, ["SUCCESS", "SUCCESS"])
        # The second job shows the dataets already exist
        self.assertListEqual(
            list(results[1].items())[0][1],
            [
                {"boundary_processor": {"boundary_folder": "exists"}},
                {"test_processor.version_1": {"test_processor - exists": True}},
                {"provenance_processor": {"updated in backend": True}},
            ],
        )
        # Assert we only get a single package output
        assert_package(
            LOCALFS_STORAGE_BACKEND_ROOT,
            "gambia",
            JOB_SUBMIT_DATA_GAMBIA_TEST_PROC["processors"],
        )
        # remove_tree(LOCALFS_STORAGE_BACKEND_ROOT, packages=["gambia"])
