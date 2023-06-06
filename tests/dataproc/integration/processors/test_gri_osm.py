"""
Unit tests for GRI OSM
"""
import os
import unittest
import shutil

from tests.helpers import (
    assert_exists_awss3,
    load_country_geojson,
    assert_datapackage_resource,
    clean_packages,
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
    DummyTaskExecutor,
)
from dataproc import Boundary
from dataproc.processors.core.gri_osm.roads_and_rail_version_1 import (
    Processor,
    Metadata,
)
from dataproc.backends.storage import init_storage_backend
from dataproc.backends.storage.awss3 import S3Manager
from config import (
    PACKAGES_HOST_URL,
    S3_REGION,
    STORAGE_BACKEND,
    S3_BUCKET,
    TEST_GRI_OSM,
)


class TestGRIOSMProcessor(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        cls.test_processing_data_dir = os.path.join(
            LOCAL_FS_PROCESSING_DATA_TOP_DIR, Metadata().name, Metadata().version
        )
        os.makedirs(cls.test_processing_data_dir, exist_ok=True)
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)
        cls.storage_backend = init_storage_backend(STORAGE_BACKEND)
        # Ensure clean test-env
        # Tmp and Source data
        shutil.rmtree(cls.test_processing_data_dir)
        # Package data
        clean_packages(
            STORAGE_BACKEND,
            cls.storage_backend,
            s3_bucket=S3_BUCKET,
            s3_region=S3_REGION,
            packages=["gambia"],
        )

    @classmethod
    def tearDownClass(cls):
        # Tmp and Source data
        shutil.rmtree(cls.test_processing_data_dir)
        # Package data
        clean_packages(
            STORAGE_BACKEND,
            cls.storage_backend,
            s3_bucket=S3_BUCKET,
            s3_region=S3_REGION,
            packages=["gambia"],
        )

    def setUp(self):
        self.task_executor = DummyTaskExecutor()
        self.meta = Metadata()
        self.proc = Processor(
            self.meta,
            self.boundary,
            self.storage_backend,
            self.task_executor,
            self.test_processing_data_dir,
        )

    def test_processor_init(self):
        """"""
        self.assertIsInstance(self.proc, Processor)

    def test_context_manager(self):
        """"""
        with Processor(
            self.meta,
            self.boundary,
            self.storage_backend,
            self.task_executor,
            self.test_processing_data_dir,
        ) as proc:
            self.assertIsInstance(proc, Processor)

    def test_context_manager_cleanup_on_error(self):
        """"""
        with Processor(
            self.meta,
            self.boundary,
            self.storage_backend,
            self.task_executor,
            self.test_processing_data_dir,
        ) as proc:
            test_fpath = os.path.join(proc.tmp_processing_folder, "testfile")
            # Add a file into the tmp processing backend
            with open(test_fpath, "w") as fptr:
                fptr.write("data")
        self.assertFalse(os.path.exists(test_fpath))

    def test_meta_init(self):
        """"""
        self.assertIsInstance(self.meta, Metadata)
        self.assertNotEqual(self.meta.name, "")
        self.assertNotEqual(self.meta.version, "")
        self.assertNotEqual(self.meta.dataset_name, "")

    def test_generate(self):
        """E2E generate test - fetch, crop, push"""
        # Remove the final package artifacts (but keep the test data artifacts if they exist)
        if TEST_GRI_OSM is False:
            self.skipTest(f"Skipping GRI OSM due to TEST_GRI_OSM == {TEST_GRI_OSM}")
        clean_packages(
            STORAGE_BACKEND,
            self.storage_backend,
            s3_bucket=S3_BUCKET,
            s3_region=S3_REGION,
            packages=["gambia"],
        )
        prov_log = self.proc.generate()
        # # Assert the log contains a succesful entries
        self.assertTrue(prov_log[f"{self.proc.metadata.name} - crop completed"])
        self.assertTrue(
            prov_log[f"{self.proc.metadata.name} - move to storage success"]
        )
        # # Collect the URI for the final Raster
        final_uri = prov_log[f"{self.proc.metadata.name} - result URI"]
        if STORAGE_BACKEND == "localfs":
            self.assertTrue(
                os.path.exists(
                    final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR)
                )
            )
        elif STORAGE_BACKEND == "awss3":
            with S3Manager(
                *self.storage_backend._parse_env(), region=S3_REGION
            ) as s3_fs:
                assert_exists_awss3(
                    s3_fs,
                    final_uri.replace(PACKAGES_HOST_URL, S3_BUCKET),
                )
        else:
            pass
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
