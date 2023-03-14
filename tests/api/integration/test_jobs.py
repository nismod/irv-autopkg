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
import shutil

import requests

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE
from tests.helpers import build_route, remove_tree, assert_package
from tests.dataproc.integration.processors import (
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
)
from tests.helpers import (
    clean_packages,
    assert_package_awss3,
)
from dataproc.backends.storage import init_storage_backend
from dataproc.backends.storage.awss3 import AWSS3StorageBackend, S3Manager
from config import (
    STORAGE_BACKEND,
    S3_BUCKET,
    S3_REGION,
    PACKAGES_HOST_URL,
)


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
}

JOB_SUBMIT_DATA_ZAMBIA_TEST_PROC = {
    "boundary_name": "zambia",
    "processors": ["test_processor.version_1"],
}

JOB_SUBMIT_DATA_GHANA_TEST_PROC = {
    "boundary_name": "ghana",
    "processors": ["test_processor.version_1"],
}

JOB_SUBMIT_DATA_SSUDAN_NE_VECTOR_PROC = {
    "boundary_name": "ssudan",
    "processors": ["natural_earth_vector.version_1"],
}

JOB_SUBMIT_DATA_GUINEA_ALL_PROC = {
    "boundary_name": "guineabissau",
    "processors": [
		"natural_earth_raster.version_1",
		"natural_earth_vector.version_1",
		"wri_powerplants.version_130",
    ],
} # Omits OSM and other requiring large downloads


class TestProcessingJobs(unittest.TestCase):

    """
    These tests require API and Celery Worker to be running (with redis)
    """

    @classmethod
    def setUpClass(cls):
        cls.max_job_await = 20  # secs
        cls.storage_backend = init_storage_backend(STORAGE_BACKEND)
        clean_packages(
            STORAGE_BACKEND,
            cls.storage_backend,
            s3_bucket=S3_BUCKET,
            s3_region=S3_REGION,
            packages=["gambia", "zambia", "ssudan"],
        )

    @classmethod
    def tearDownClass(cls):
        # Package data
        clean_packages(
            STORAGE_BACKEND,
            cls.storage_backend,
            s3_bucket=S3_BUCKET,
            s3_region=S3_REGION,
            packages=["gambia", "zambia", "ssudan"],
        )

    def test_get_job_no_exist(self):
        """"""
        # Note - non existant tasks that are assumed as pending (i.e. a random UUID
        expected_code = 200
        expected_status = "PENDING"
        noexist_job = str(uuid4())
        route = build_route(JOB_STATUS_ROUTE.format(job_id=noexist_job))
        response = requests.get(route)
        self.assertEqual(response.status_code, expected_code)
        self.assertEqual(response.json()["job_group_status"], expected_status)

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
        expected_code = 202
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_GAMBIA_TEST_PROC)
        self.assertEqual(response.status_code, expected_code)
        self.assertIn("job_id", response.json().keys())
        job_id = response.json()["job_id"]
        # Await job completion
        start = time()
        while True:
            route = build_route(JOB_STATUS_ROUTE.format(job_id=job_id))
            response = requests.get(route)
            if response.json()["job_group_processors"]:
                self.assertEqual(
                    response.json()["job_group_processors"][0]["job_id"], job_id
                )
            if not response.json()["job_group_status"] == "PENDING":
                # Final await for any S3 refreshing backend
                sleep(1.0)
                break
            sleep(1.0)
            if (time() - start) > self.max_job_await:
                self.fail("max await reached")
        self.assertEqual(response.json()["job_group_status"], "COMPLETE")
        # Assert the package integrity, including submitted processor
        if STORAGE_BACKEND == "localfs":
            assert_package(
                LOCAL_FS_PACKAGE_DATA_TOP_DIR,
                "gambia",
            )
        elif STORAGE_BACKEND == "awss3":
            assert_package_awss3(
                self.storage_backend,
                "gambia",
                expected_processor_versions=JOB_SUBMIT_DATA_GAMBIA_TEST_PROC["processors"],
            )

    def test_submit_job_already_processing_using_test_processor(self):
        """
        Submission of a multiple jobs containing the same boundary and 
            processor while one is already executing (test processor)
        """
        max_wait = 60  # secs
        dup_processors_to_submit = 8
        expected_responses = [202 for i in range(dup_processors_to_submit)]
        route = build_route(JOBS_BASE_ROUTE)
        responses = []
        for _ in range(dup_processors_to_submit):
            response = requests.post(route, json=JOB_SUBMIT_DATA_ZAMBIA_TEST_PROC)
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
            for idx, job_id in enumerate(submitted_ids):
                route = build_route(JOB_STATUS_ROUTE.format(job_id=job_id))
                response = requests.get(route)
                completed[idx] = (
                    True
                    if response.json()["job_group_status"] == "COMPLETE"
                    else False
                )
                if len(response.json()["job_group_processors"]) > 0:
                    results[idx] = response.json()["job_group_processors"][0]
                statuses[idx] = response.json()["job_group_status"]
                sleep(0.01)
            if (time() - start) > max_wait:
                self.fail("max await time exceeded")
            sleep(0.5)
        # Jobs completed successfully
        self.assertEqual(statuses, ["COMPLETE" for i in range(dup_processors_to_submit)])
        test_proc_results = []
        for result in results:
            test_proc_results.append(result["job_result"])
        # Test Processor Success
        self.assertIn(
            {
                "test_processor - move to storage success": True,
                "test_processor - result URI": f"{PACKAGES_HOST_URL}/zambia/datasets/test_processor/version_1/data/zambia_test.tif",
                "datapackage": {
                    "name": "test_processor",
                    "version": "version_1",
                    "path": [f"{PACKAGES_HOST_URL}/zambia/datasets/test_processor/version_1/data/zambia_test.tif"],
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
        self.assertTrue(len(set([json.dumps(i) for i in test_proc_results])), 1)

        # Assert we only get a single package output
        if STORAGE_BACKEND == "localfs":
            assert_package(
                LOCAL_FS_PACKAGE_DATA_TOP_DIR,
                "zambia",
            )
        elif STORAGE_BACKEND == "awss3":
            assert_package_awss3(
                self.storage_backend,
                "zambia",
                expected_processor_versions=JOB_SUBMIT_DATA_ZAMBIA_TEST_PROC["processors"],
            )

    def test_submit_job_already_processing_using_ne_vector_processor(self):
        """
        Submission of a second job containing the same boundary and processor while one is already executing (ne vector)
        """
        max_wait = 30  # secs
        dup_processors_to_submit = 2
        expected_responses = [202 for i in range(dup_processors_to_submit)]
        route = build_route(JOBS_BASE_ROUTE)
        responses = []
        for _ in range(dup_processors_to_submit):
            response = requests.post(route, json=JOB_SUBMIT_DATA_SSUDAN_NE_VECTOR_PROC)
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
                self.fail("max await reached")
            for idx, job_id in enumerate(submitted_ids):
                route = build_route(JOB_STATUS_ROUTE.format(job_id=job_id))
                response = requests.get(route)
                completed[idx] = (
                    True
                    if response.json()["job_group_status"] == "COMPLETE"
                    else False
                )
                if len(response.json()["job_group_processors"]) > 0:
                    results[idx] = response.json()["job_group_processors"][0]
                statuses[idx] = response.json()["job_group_status"]
                sleep(0.01)
            sleep(0.5)
        # Jobs completed successfully
        self.assertEqual(statuses, ["COMPLETE" for i in range(dup_processors_to_submit)])
        # Between the two sets of results there should be success for
        # both boundaries and test_processor
        test_proc_results = []
        for result in results:
            test_proc_results.append(result["job_result"])
        # Correct total processing results
        self.assertEqual(len(test_proc_results), dup_processors_to_submit)
        # Should have only ran fully once - the rest should be exists
        count_processed_e2e = 0
        count_processed_e2e_key = "natural_earth_vector - loaded NE Roads to PG"
        for i in test_proc_results:
            if count_processed_e2e_key in i.keys():
                count_processed_e2e+=1
        self.assertEqual(count_processed_e2e, 1)
        # Test Processor Success keys all exist
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

        if STORAGE_BACKEND == "localfs":
            assert_package(
                LOCAL_FS_PACKAGE_DATA_TOP_DIR,
                "ssudan",
            )
        elif STORAGE_BACKEND == "awss3":
            assert_package_awss3(
                self.storage_backend,
                "ssudan",
                expected_processor_versions=JOB_SUBMIT_DATA_SSUDAN_NE_VECTOR_PROC["processors"],
            )

    def test_all_processors(self):
        """Submission of a job containing all processors"""
        max_total_await = 120
        expected_code = 202
        route = build_route(JOBS_BASE_ROUTE)
        response = requests.post(route, json=JOB_SUBMIT_DATA_GUINEA_ALL_PROC)
        self.assertEqual(response.status_code, expected_code)
        self.assertIn("job_id", response.json().keys())
        job_id = response.json()["job_id"]
        # Await job completion
        start = time()
        all_processors_listed = []
        while True:
            route = build_route(JOB_STATUS_ROUTE.format(job_id=job_id))
            response = requests.get(route)
            if response.json()["job_group_processors"]:
                # All submitted processor names are covered in job group
                all_processors_listed.append(
                    set([i['processor_name'] for i in response.json()["job_group_processors"]]) == set(JOB_SUBMIT_DATA_GUINEA_ALL_PROC["processors"])
                )
            if not response.json()["job_group_status"] == "PENDING":
                # Final await for any S3 refreshing backend
                sleep(1.0)
                break
            sleep(1.0)
            if (time() - start) > max_total_await:
                self.fail("max await breached")
        # Check all the processors were covered in the job group (with some allowance for start-up)
        self.assertTrue(all_processors_listed.count(False) < 0.2*len(all_processors_listed))
        response = requests.get(route)
        # Job progress should all be null
        self.assertTrue(not any([i['job_progress'] for i in response.json()["job_group_processors"]]))
        # Job status should be completed and successful for all
        self.assertEqual(response.json()["job_group_status"], "COMPLETE")
        self.assertSetEqual(set([i['job_status'] for i in response.json()["job_group_processors"]]), set(["SUCCESS"]))
        # Assert the package integrity, including submitted processors
        if STORAGE_BACKEND == "localfs":
            assert_package(
                LOCAL_FS_PACKAGE_DATA_TOP_DIR,
                "guineabissau",
            )
        elif STORAGE_BACKEND == "awss3":
            assert_package_awss3(
                self.storage_backend,
                "guineabissau",
                expected_processor_versions=JOB_SUBMIT_DATA_GUINEA_ALL_PROC["processors"],
            )