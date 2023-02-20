"""
Unit tests for GridFinder
"""
import os
import unittest
import shutil

from dataproc.backends import LocalFSStorageBackend
from dataproc import Boundary
from dataproc.processors.core.gridfinder.version_1 import (
    Processor,
    Metadata,
)
from dataproc.helpers import assert_geotiff, assert_vector_file
from tests.helpers import (
    load_country_geojson,
    assert_raster_bounds_correct,
    assert_datapackage_resource,
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
    DummyTaskExecutor,
)
from config import PACKAGES_HOST_URL

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
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_PACKAGE_DATA_TOP_DIR)

    @classmethod
    def tearDownClass(cls):
        # Tmp and Source data
        shutil.rmtree(cls.test_processing_data_dir, ignore_errors=True)
        # Package data
        shutil.rmtree(
            os.path.join(cls.storage_backend.top_level_folder_path, "gambia"),
            ignore_errors=True,
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
            "grid.gpkg": "EPSG:4326",
            "lv.tif": "ESRI:54009",
            "targets.tif": "EPSG:4326",
        }
        try:
            shutil.rmtree(
                os.path.join(self.storage_backend.top_level_folder_path, "gambia")
            )
        except FileNotFoundError:
            pass
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
        for final_uri in final_uris.split(","):
            fname = os.path.basename(final_uri)
            if os.path.splitext(fname)[1] == ".tif":
                # # Assert the geotiffs are valid
                assert_geotiff(
                    final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
                    check_crs=expected_crs[fname],
                )
                # # Assert the envelopes
                assert_raster_bounds_correct(
                    final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
                    self.boundary["envelope_geojson"],
                )
            else:
                assert_vector_file(
                    final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
                    (163, 2),
                    expected_crs=expected_crs[fname],
                )
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
