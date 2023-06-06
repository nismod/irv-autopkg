"""
Unit tests for JRC GHSL Population
"""
import os
import unittest
import shutil

from tests.helpers import (
    load_country_geojson,
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
from dataproc.processors.core.jrc_ghsl_population.r2022_epoch2020_1km import (
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


class TestJRCGHSLPopR2022E20201KMProcessor(unittest.TestCase):
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
        shutil.rmtree(cls.test_processing_data_dir, ignore_errors=True)
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
        # Change the expected zip URL so we just download the tile over gambia
        self.proc.zip_url = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2022A/GHS_POP_E2020_GLOBE_R2022A_54009_1000/V1-0/tiles/GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0_R8_C17.zip"
        self.proc.source_fnames = [
            "GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0_R8_C17.zip"
        ]

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
        final_uri = prov_log[f"{self.proc.metadata.name} - result URI"]
        if STORAGE_BACKEND == "localfs":
            assert_raster_output(
                self.boundary["envelope_geojson"],
                final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
                check_crs="ESRI:54009",
                pixel_check_raster_fpath=self.proc._fetch_source(),
            )
        elif STORAGE_BACKEND == "awss3":
            with S3Manager(
                *self.storage_backend._parse_env(), region=S3_REGION
            ) as s3_fs:
                assert_raster_output(
                    self.boundary["envelope_geojson"],
                    s3_fs=s3_fs,
                    s3_raster_fpath=final_uri.replace(PACKAGES_HOST_URL, S3_BUCKET),
                    check_crs="ESRI:54009",
                    pixel_check_raster_fpath=self.proc._fetch_source(),
                )
        else:
            pass
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
