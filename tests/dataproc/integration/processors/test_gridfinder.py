"""
Unit tests for GridFinder
"""
import os
import unittest
import shutil


from tests.helpers import (
    load_country_geojson,
    assert_vector_output,
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
from dataproc.processors.core.gridfinder.version_1 import (
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

TEST_DATA_DIR = os.path.join(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
    "data",
    "gridfinder",
)


class TestGridFinderV1Processor(unittest.TestCase):
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
        """
        E2E generate test - fetch, crop, push

        We are using locally sourced test-files.
        """
        expected_crs = {
            "gridfinder-version_1-grid-gambia.gpkg": "EPSG:4326",
            "gridfinder-version_1-lv-gambia.tif": "ESRI:54009",
            "gridfinder-version_1-targets-gambia.tif": "EPSG:4326",
        }
        result_source_map = {
            "gridfinder-version_1-lv-gambia.tif": "lv.tif",
            "gridfinder-version_1-targets-gambia.tif": "targets.tif",
        }
        clean_packages(
            STORAGE_BACKEND,
            self.storage_backend,
            s3_bucket=S3_BUCKET,
            s3_region=S3_REGION,
            packages=["gambia"],
        )
        # Move test-data into the expected source folder
        for _file in os.scandir(TEST_DATA_DIR):
            shutil.copy(
                os.path.join(TEST_DATA_DIR, _file.name),
                os.path.join(self.proc.source_folder, _file.name),
            )
        self.assertTrue(self.proc._all_source_exists())
        prov_log = self.proc.generate()
        # Assert the log contains successful entries
        self.assertTrue(
            prov_log[f"{self.proc.metadata.name} - move to storage success"]
        )
        # Collect the URIs for the final Raster
        final_uris = prov_log[f"{self.proc.metadata.name} - result URIs"]
        self.assertEqual(len(final_uris.split(",")), self.proc.total_expected_files)
        # Collect the original source fpaths for pixel assertion
        for final_uri in final_uris.split(","):
            fname = os.path.basename(final_uri)
            if os.path.splitext(fname)[1] == ".tif":
                # Match original source raster for pixel assertion
                if STORAGE_BACKEND == "localfs":
                    assert_raster_output(
                        self.boundary["envelope_geojson"],
                        final_uri.replace(
                            PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR
                        ),
                        check_crs=expected_crs[fname],
                        pixel_check_raster_fpath=os.path.join(
                            self.proc.source_folder, result_source_map[fname]
                        ),
                    )
                elif STORAGE_BACKEND == "awss3":
                    with S3Manager(
                        *self.storage_backend._parse_env(), region=S3_REGION
                    ) as s3_fs:
                        assert_raster_output(
                            self.boundary["envelope_geojson"],
                            s3_fs=s3_fs,
                            s3_raster_fpath=final_uri.replace(
                                PACKAGES_HOST_URL, S3_BUCKET
                            ),
                            check_crs=expected_crs[fname],
                            pixel_check_raster_fpath=os.path.join(
                                self.proc.source_folder, result_source_map[fname]
                            ),
                        )
                else:
                    pass
            else:
                if STORAGE_BACKEND == "localfs":
                    assert_vector_output(
                        (84, 2),
                        expected_crs[fname],
                        local_vector_fpath=final_uri.replace(
                            PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR
                        ),
                    )
                elif STORAGE_BACKEND == "awss3":
                    with S3Manager(
                        *self.storage_backend._parse_env(), region=S3_REGION
                    ) as s3_fs:
                        assert_vector_output(
                            (84, 2),
                            expected_crs[fname],
                            s3_fs=s3_fs,
                            s3_vector_fpath=final_uri.replace(
                                PACKAGES_HOST_URL, S3_BUCKET
                            ),
                        )
                else:
                    pass
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
