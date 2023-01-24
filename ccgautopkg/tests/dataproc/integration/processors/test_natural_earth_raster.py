"""
Unit tests for Natural Earth Raster Processor
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from tests.helpers import (
    load_country_geojson,
    assert_raster_bounds_correct,
    setup_test_data_paths,
    assert_datapackage_resource,
)
from dataproc import Boundary
from dataproc.processors.core.natural_earth_raster.version_1 import (
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
            LOCAL_FS_PROCESSING_DATA_TOP_DIR, "natural_earth_raster"
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
        setup_test_data_paths(self.proc, self.test_processing_data_dir)
        self.meta = Metadata()

    def test_processor_init(self):
        """"""
        self.assertIsInstance(self.proc, Processor)

    def test_context_manager(self):
        """"""
        with Processor(self.boundary, self.storage_backend) as proc:
            self.assertIsInstance(proc, Processor)

    def test_context_manager_cleanup_on_error(self):
        """"""
        with Processor(self.boundary, self.storage_backend) as proc:
            setup_test_data_paths(self.proc, self.test_processing_data_dir)
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
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
