"""
Unit tests for ISIMP Drought
"""
import os
import unittest
import shutil

from tests.helpers import (
    load_country_geojson,
    assert_raster_bounds_correct,
    assert_datapackage_resource,
    clean_packages,
    assert_raster_output,
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
    DummyTaskExecutor,
)
from dataproc import Boundary
from dataproc.processors.core.isimp_drought.version_1 import (
    Processor,
    Metadata,
)
from dataproc.storage import init_storage_backend
from dataproc.storage.awss3 import S3Manager
from config import (
    PACKAGES_HOST_URL,
    S3_REGION,
    STORAGE_BACKEND,
    S3_BUCKET,
)

TEST_VERSION_1_SOURCE_FILES = [
    "lange2020_clm45_miroc5_ewembi_rcp60_2005soc_co2_led_global_annual_2006_2099_2080_occurrence.tif",
    "lange2020_lpjml_miroc5_ewembi_rcp60_2005soc_co2_led_global_annual_2006_2099_2080_occurrence.tif",
    "lange2020_clm45_gfdl-esm2m_ewembi_rcp60_2005soc_co2_led_global_annual_2006_2099_2030_occurrence.tif",
]

TEST_DATA_DIR = os.path.join(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
    "data",
    "isimp_drought_v1",
)


class TestISIMPDroughtV1Processor(unittest.TestCase):
    """ """

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
        # Move test-data into the expected source folder
        for _file in os.scandir(TEST_DATA_DIR):
            shutil.copy(
                os.path.join(TEST_DATA_DIR, _file.name),
                os.path.join(self.proc.source_folder, _file.name),
            )
        # Limit expected source files
        self.proc.source_files = TEST_VERSION_1_SOURCE_FILES
        self.proc.total_expected_files = len(TEST_VERSION_1_SOURCE_FILES)
        prov_log = self.proc.generate()
        # Assert the log contains successful entries
        self.assertTrue(
            prov_log[f"{self.proc.metadata.name} - move to storage success"]
        )
        # Collect the URIs for the final Rasters
        final_uris = prov_log[f"{self.proc.metadata.name} - result URIs"]
        self.assertEqual(len(final_uris.split(",")), self.proc.total_expected_files)
        # Collect the original source fpaths for pixel assertion
        source_fpaths = self.proc._fetch_source()
        for idx, final_uri in enumerate(final_uris.split(",")):
            if STORAGE_BACKEND == "localfs":
                assert_raster_output(
                    self.boundary["envelope_geojson"],
                    final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
                    check_crs="EPSG:4326",
                    tolerence=0.5,
                    pixel_check_raster_fpath=source_fpaths[idx],
                )
            elif STORAGE_BACKEND == "awss3":
                with S3Manager(
                    *self.storage_backend._parse_env(), region=S3_REGION
                ) as s3_fs:
                    assert_raster_output(
                        self.boundary["envelope_geojson"],
                        s3_fs=s3_fs,
                        s3_raster_fpath=final_uri.replace(PACKAGES_HOST_URL, S3_BUCKET),
                        check_crs="EPSG:4326",
                        tolerence=0.5,
                        pixel_check_raster_fpath=source_fpaths[idx],
                    )
            else:
                pass
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
