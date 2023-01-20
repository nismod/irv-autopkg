"""
Unit tests for WRI Aq   ueduct
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from tests.helpers import load_country_geojson, assert_raster_bounds_correct
from dataproc import Boundary
from dataproc.processors.core.wri_aqueduct.version_1 import (
    Processor,
    Metadata,
)
from dataproc.helpers import assert_geotiff

LOCAL_FS_DATA_TOP_DIR = os.path.join(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
    "data",
    "tmp",
)


class TestWRIAqueductProcessor(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(
            LOCAL_FS_DATA_TOP_DIR, "wri_aqueduct"
        )
        os.makedirs(cls.test_data_dir, exist_ok = True)
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_DATA_TOP_DIR)

    @classmethod
    def tearDownClass(cls):
        # shutil.rmtree(cls.test_data_dir)
        pass

    def setUp(self):
        self.proc = Processor(self.boundary, self.storage_backend)
        self.meta = Metadata()

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
        try:
            shutil.rmtree(os.path.join(self.storage_backend.top_level_folder_path, "gambia"))
        except FileNotFoundError:
            pass
        prov_log = self.proc.generate()
        # Assert the log contains a succesful entries
        self.assertTrue(prov_log[f"{Metadata().name} - move to storage success"])
        # Collect the URI for the final Raster
        final_uris = prov_log[f"{Metadata().name} - result URIs"]
        self.assertEqual(len(final_uris.split(',')), self.proc.total_expected_files)
        for final_uri in final_uris.split(','):
            # # Assert the geotiffs are valid
            assert_geotiff(final_uri)
            # # Assert the envelopes
            assert_raster_bounds_correct(final_uri, self.boundary["envelope_geojson"])
