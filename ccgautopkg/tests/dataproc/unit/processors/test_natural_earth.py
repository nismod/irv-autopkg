"""
Unit tests for Natural Earth Processor
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from dataproc.backends.processing.localfs import LocalFSProcessingBackend
from tests.helpers import load_country_geojson, assert_raster_bounds_correct
from dataproc import Boundary
from dataproc.processors.core.natural_earth.version_1 import Processor, Metadata

LOCAL_FS_DATA_TOP_DIR = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.abspath(__file__)
                )
            )
        )
    ), "data", "tmp"
)

class TestNaturalEarthProcessor(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.join(
            LOCAL_FS_DATA_TOP_DIR, "natural_earth"
        )
        if not os.path.exists(cls.test_data_dir):
            os.mkdir(cls.test_data_dir)
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_DATA_TOP_DIR)
        cls.processing_backend = LocalFSProcessingBackend(LOCAL_FS_DATA_TOP_DIR)
    
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_data_dir)
        shutil.rmtree(os.path.join(cls.storage_backend.top_level_folder_path, "gambia"))

    def setUp(self):
        self.proc = Processor(self.boundary, self.storage_backend, self.processing_backend)
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

    def test_fetch_source(self):
        """Test the fetching of source zip, unpacking and assertion"""
        gtiff_fpath = self.proc._fetch_source()
        self.assertTrue(
            os.path.exists(
                gtiff_fpath
            )
        )

    def test_generate(self):
        """E2E generate test - fetch, crop, push"""
        prov_log = self.proc.generate()
        # Assert the log contains a succesful entries
        self.assertTrue(prov_log[f"{Metadata().name} - crop success"])
        self.assertTrue(prov_log[f"{Metadata().name} - move to storage success"])
        # Collect the URI for the final Raster
        final_uri = prov_log[f"{Metadata().name} - result URI"]
        # Assert the geotiff is valid
        self.processing_backend.assert_geotiff(
            final_uri
        )
        # Assert the envelope
        assert_raster_bounds_correct(final_uri, self.boundary['envelope_geojson'])

