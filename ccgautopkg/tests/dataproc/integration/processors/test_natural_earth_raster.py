"""
Unit tests for Natural Earth Raster Processor
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from tests.helpers import load_country_geojson, assert_raster_bounds_correct
from dataproc import Boundary
from dataproc.processors.core.test_natural_earth_raster.version_1 import (
    Processor,
    Metadata,
)
from dataproc.helpers import assert_geotiff
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
)


class TestNaturalEarthRasterProcessor(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        cls.test_processing_data_dir = os.path.join(
            LOCAL_FS_PROCESSING_DATA_TOP_DIR, "test_natural_earth_raster"
        )
        os.makedirs(cls.test_processing_data_dir, exist_ok=True)
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_PACKAGE_DATA_TOP_DIR)

    @classmethod
    def tearDownClass(cls):
        # Tmp and Source data
        shutil.rmtree(cls.test_processing_data_dir)
        # Package data
        shutil.rmtree(os.path.join(cls.storage_backend.top_level_folder_path, "gambia"))

    def setUp(self):
        self.proc = Processor(self.boundary, self.storage_backend)
        # __NOTE__: Reset the paths helper to reflect the test environment for processing root
        self.proc.paths_helper.top_level_folder_path = self.test_processing_data_dir
        self.proc.source_folder = self.proc.paths_helper.build_absolute_path(
            "source_data"
        )
        self.proc.tmp_processing_folder = self.proc.paths_helper.build_absolute_path(
            "tmp"
        )
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
        self.assertTrue(os.path.exists(gtiff_fpath))

    def test_generate(self):
        """E2E generate test - fetch, crop, push"""
        try:
            shutil.rmtree(
                os.path.join(self.storage_backend.top_level_folder_path, "gambia")
            )
        except FileNotFoundError:
            pass
        prov_log = self.proc.generate()
        # Assert the log contains a succesful entries
        self.assertTrue(prov_log[f"{Metadata().name} - crop success"])
        self.assertTrue(prov_log[f"{Metadata().name} - move to storage success"])
        # Collect the URI for the final Raster
        final_uri = prov_log[f"{Metadata().name} - result URI"]
        # Assert the geotiff is valid
        assert_geotiff(final_uri)
        # Assert the envelope
        assert_raster_bounds_correct(final_uri, self.boundary["envelope_geojson"])
