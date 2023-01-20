"""
Unit tests for GRI OSM
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from tests.helpers import (
    load_country_geojson
)
from dataproc import Boundary
from dataproc.processors.core.gri_osm.version_1 import (
    Processor,
    Metadata,
)
from config import get_db_uri_sync, API_DB_NAME, LOCALFS_PROCESSING_BACKEND_ROOT

LOCAL_FS_DATA_TOP_DIR = os.path.join(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
    "data",
    "tmp",
)


class TestGRIOSMProcessor(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = None
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_DATA_TOP_DIR)

    @classmethod
    def tearDownClass(cls):
        # Cleans processing data
        try:
            if cls.test_data_dir:
                shutil.rmtree(cls.test_data_dir)
        except FileNotFoundError:
            print("Skipped removing test data tree for", cls.__name__)

    def setUp(self):
        self.proc = Processor(self.boundary, self.storage_backend)
        self.meta = Metadata()
        # Set test data directory as the same as configuration in processor
        self.test_data_dir = self.proc.paths_helper.top_level_folder_path

    def test_processor_init(self):
        """"""
        self.assertIsInstance(self.proc, Processor)

    def test_meta_init(self):
        """"""
        self.assertIsInstance(self.meta, Metadata)
        self.assertNotEqual(self.meta.name, "")
        self.assertNotEqual(self.meta.version, "")
        self.assertNotEqual(self.meta.dataset_name, "")

    def test_generate(self):
        """E2E generate test - fetch, crop, push"""
        # Remove the final package artifacts (but keep the test data artifacts if they exist)
        try:
            shutil.rmtree(os.path.join(self.storage_backend.top_level_folder_path, "gambia"))
        except FileNotFoundError:
            pass
        prov_log = self.proc.generate()
        # # Assert the log contains a succesful entries
        self.assertTrue(prov_log[f"{Metadata().name} - crop completed"])
        self.assertTrue(prov_log[f"{Metadata().name} - move to storage success"])
        # # Collect the URI for the final Raster
        final_uri = prov_log[f"{Metadata().name} - result URI"]
        # Assert the file exists
        self.assertTrue(os.path.exists(final_uri))
