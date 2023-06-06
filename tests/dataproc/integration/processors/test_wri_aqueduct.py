"""
Unit tests for WRI Aqueduct
"""
import os
import unittest
import shutil

from tests.helpers import (
    load_country_geojson,
    assert_raster_bounds_correct,
    setup_test_data_paths,
    assert_raster_output,
    assert_datapackage_resource,
    clean_packages,
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
    DummyTaskExecutor,
)
from dataproc import Boundary
from dataproc.helpers import tiffs_in_folder
from dataproc.processors.core.wri_aqueduct.version_2 import (
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
)


class TestWRIAqueductProcessor(unittest.TestCase):
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
            LOCAL_FS_PROCESSING_DATA_TOP_DIR,
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
        clean_packages(
            STORAGE_BACKEND,
            self.storage_backend,
            s3_bucket=S3_BUCKET,
            s3_region=S3_REGION,
            packages=["gambia"],
        )
        # Limit the files to be downloaded  in the fetcher
        self.proc.total_expected_files = 1
        prov_log = self.proc.generate()
        # Assert the log contains successful entries
        self.assertTrue(
            prov_log[f"{self.proc.metadata.name} - move to storage success"]
        )
        # Collect the URIs for the final Raster
        final_uris = prov_log[f"{self.proc.metadata.name} - result URIs"]
        self.assertEqual(len(final_uris.split(",")), self.proc.total_expected_files)
        # Collect the original source fpaths for pixel assertion
        source_tiffs = tiffs_in_folder(self.proc.source_folder)
        for idx, final_uri in enumerate(final_uris.split(",")):
            if STORAGE_BACKEND == "localfs":
                assert_raster_output(
                    self.boundary["envelope_geojson"],
                    final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
                    pixel_check_raster_fpath=os.path.join(
                        self.proc.source_folder, source_tiffs[idx]
                    ),
                )
            elif STORAGE_BACKEND == "awss3":
                with S3Manager(
                    *self.storage_backend._parse_env(), region=S3_REGION
                ) as s3_fs:
                    assert_raster_output(
                        self.boundary["envelope_geojson"],
                        s3_fs=s3_fs,
                        s3_raster_fpath=final_uri.replace(PACKAGES_HOST_URL, S3_BUCKET),
                        pixel_check_raster_fpath=os.path.join(
                            self.proc.source_folder, source_tiffs[idx]
                        ),
                    )
            else:
                pass
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
