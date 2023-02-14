"""
Tests for Processing Jobs
"""

import os
import sys
import inspect
import unittest
from uuid import uuid4
from time import time, sleep
import json

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE
from tests.helpers import build_route, remove_tree, assert_package
from tests.dataproc.integration.processors import (
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
)
from config import PACKAGES_HOST_URL

JOB_SUBMIT_DATA_BOUNDARY_NOEXIST = {
    "boundary_name": "noexist",
    "processors": ["natural_earth_raster.version_1"],
}

JOB_SUBMIT_DATA_PROC_NOEXIST = {
    "boundary_name": "gambia",
    "processors": ["noexist.version_1"],
}

JOB_SUBMIT_DATA_PROC_DUP = {
    "boundary_name": "gambia",
    "processors": [
        "natural_earth_raster.version_1",
        "natural_earth_raster.version_1",
    ],
}

JOB_SUBMIT_DATA_GAMBIA_TEST_PROC = {
    "boundary_name": "gambia",
    "processors": ["test_processor.version_1"],
}  # Awaits 5 secs

JOB_SUBMIT_DATA_GAMBIA_NE_VECTOR_PROC = {
    "boundary_name": "gambia",
    "processors": ["natural_earth_vector.version_1"],
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
        expected_status = "PENDING"
        noexist_job = str(uuid4())
        route = build_route(JOB_STATUS_ROUTE.format(job_id=noexist_job))
        response = requests.get(route)
        self.assertEqual(response.status_code, expected_code)
        self.assertEqual(response.json()["job_status"], expected_status)

    def test_submit_job_no_such_boundary(self):
        """Submission of a job against a boundary that doesnt exist"""
        expected_code = 400
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_BOUNDARY_NOEXIST)
        self.assertEqual(response.status_code, expected_code)
        self.assertDictEqual(
            response.json(), {"detail": "Requested boundary noexist could not be found"}
        )

    def test_submit_job_no_such_processor(self):
        """Submission of a job against a processor that doesnt exist"""
        expected_code = 400
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_PROC_NOEXIST)
        self.assertEqual(response.status_code, expected_code)
        self.assertDictEqual(
            response.json(),
            {
                "detail": f"Invalid processor.version: {JOB_SUBMIT_DATA_PROC_NOEXIST['processors'][0]}"
            },
        )

    def test_submit_job_duplicate_processor(self):
        """Submission of a job with duplicate processors in the request"""
        expected_code = 422
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_PROC_DUP)
        self.assertEqual(response.status_code, expected_code)
        self.assertEqual(
            response.json()["detail"][0]["msg"], "duplicate processors not allowed"
        )

    def test_submit_job(self):
        """Simple submission and await completion of a job"""
        # Ensure the package tree is clean
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])
        expected_code = 202
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_GAMBIA_TEST_PROC)
        self.assertEqual(response.status_code, expected_code)
        self.assertIn("job_id", response.json().keys())
        job_id = response.json()["job_id"]
        # Await job completion
        start = time()
        while time() < start + self.max_job_await:
            route = build_route(JOB_STATUS_ROUTE.format(job_id=job_id))
            response = requests.get(route)
            self.assertEqual(response.json()["job_id"], job_id)
            if not response.json()["job_status"] == "PENDING":
                break
            sleep(0.2)
        self.assertEqual(response.json()["job_status"], "SUCCESS")
        # Assert the package integrity, including submitted processor
        assert_package(
            LOCAL_FS_PACKAGE_DATA_TOP_DIR,
            "gambia",
        )
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])

    def test_submit_job_already_processing_using_test_processor(self):
        """
        Submission of a second job containing
            the same boundary and processor while one is already executing
        """
        max_wait = 10  # secs
        dup_processors_to_submit = 8
        expected_responses = [202 for i in range(dup_processors_to_submit)]
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])
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
        statuses = ["NULL" for i in range(dup_processors_to_submit)]
        results = [{} for i in range(dup_processors_to_submit)]
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
        # Jobs completed successfully
        self.assertEqual(statuses, ["SUCCESS" for i in range(dup_processors_to_submit)])
        # Between the two sets of results there should be success for
        # both boundaries and test_processor
        boundary_proc_results = []
        test_proc_results = []
        for job_result in results:
            for _, task_results in job_result.items():
                for task in task_results:
                    if "boundary_processor" in task.keys():
                        boundary_proc_results.append(task["boundary_processor"])
                    if "test_processor.version_1" in task.keys():
                        test_proc_results.append(task["test_processor.version_1"])
        # Boundary success
        self.assertIn(
            {
                "boundary_folder": "created",
                "boundary_data_folder": "created",
                "boundary_index": "created",
                "boundary_license": "created",
                "boundary_version": "created",
                "boundary_datapackage": "created",
            },
            boundary_proc_results,
        )
        # Boundary success only reported once
        self.assertTrue(len(set([json.dumps(i) for i in boundary_proc_results])), 1)
        # Test Processor Success
        self.assertIn(
            {
                "test_processor - move to storage success": True,
                "test_processor - result URI": f"{PACKAGES_HOST_URL}/gambia/datasets/test_processor/version_1/data/gambia_test.tif",
                "datapackage": {
                    "name": "test_processor",
                    "version": "version_1",
                    "path": [f"{PACKAGES_HOST_URL}/gambia/datasets/test_processor/version_1/data/gambia_test.tif"],
                    "description": "A test processor for nightlights",
                    "format": "GEOPKG",
                    "bytes": 5,
                    "hashes": ["4e1243bd22c66e76c2ba9eddc1f91394e57f9f83"],
                    "license": {
                        "name": "CC-BY-4.0",
                        "path": "https://creativecommons.org/licenses/by/4.0/",
                        "title": "Creative Commons Attribution 4.0",
                    },
                    "sources": [{'title': 'nightlights', 'path': 'http://url'}],
                },
            },
            test_proc_results,
        )
        # Processor success only reported once
        self.assertTrue(len(set([json.dumps(i) for i in test_proc_results])))

        # Assert we only get a single package output
        assert_package(
            LOCAL_FS_PACKAGE_DATA_TOP_DIR,
            "gambia",
        )
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])

    def test_submit_job_already_processing_using_ne_vector_processor(self):
        """
        Submission of a second job containing
            the same boundary and processor while one is already executing
        """
        max_wait = 60  # secs
        dup_processors_to_submit = 8
        expected_responses = [202 for i in range(dup_processors_to_submit)]
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])
        route = build_route(JOBS_BASE_ROUTE)
        responses = []
        for _ in range(dup_processors_to_submit):
            response = requests.post(route, json=JOB_SUBMIT_DATA_GAMBIA_NE_VECTOR_PROC)
            responses.append(response)
        self.assertListEqual(
            [i.status_code for i in responses],
            # Both jobs have been accepted
            expected_responses,
        )
        submitted_ids = [data.json()["job_id"] for data in responses]
        # Wait for both processors to finish
        completed = [False for i in expected_responses]
        statuses = ["NULL" for i in range(dup_processors_to_submit)]
        results = [{} for i in range(dup_processors_to_submit)]
        start = time()
        while not all(completed):
            if (time() - start) > max_wait:
                print("Aborting waiting for jobs:", completed, statuses)
                break
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
        # Jobs completed successfully
        self.assertEqual(statuses, ["SUCCESS" for i in range(dup_processors_to_submit)])
        # Between the two sets of results there should be success for
        # both boundaries and test_processor
        boundary_proc_results = []
        test_proc_results = []
        for job_result in results:
            for _, task_results in job_result.items():
                for task in task_results:
                    if "boundary_processor" in task.keys():
                        boundary_proc_results.append(task["boundary_processor"])
                    if "natural_earth_vector.version_1" in task.keys():
                        test_proc_results.append(task["natural_earth_vector.version_1"])
        # Boundary success
        self.assertIn(
            {
                "boundary_folder": "created",
                "boundary_data_folder": "created",
                "boundary_index": "created",
                "boundary_license": "created",
                "boundary_version": "created",
                "boundary_datapackage": "created",
            },
            boundary_proc_results,
        )
        # Boundary success only reported once
        self.assertTrue(len(set([json.dumps(i) for i in boundary_proc_results])), 1)
        # Correct total processing results - including 7 exists
        self.assertEqual(len(test_proc_results), dup_processors_to_submit)
        # Test Processor Success
        self.assertIn(
            sorted([
                "natural_earth_vector - zip download path",
                "natural_earth_vector - loaded NE Roads to PG",
                "natural_earth_vector - crop completed",
                "natural_earth_vector - move to storage success",
                "natural_earth_vector - result URI",
                "natural_earth_vector - created index documentation",
                "natural_earth_vector - created license documentation",
                "datapackage"
            ]),
            [sorted(list(i.keys())) for i in test_proc_results],
        )
        # Processor success only reported once
        self.assertTrue(len(set([json.dumps(i) for i in test_proc_results])))

        # Assert we only get a single package output
        assert_package(
            LOCAL_FS_PACKAGE_DATA_TOP_DIR,
            "gambia",
        )
        remove_tree(LOCAL_FS_PACKAGE_DATA_TOP_DIR, packages=["gambia"])
