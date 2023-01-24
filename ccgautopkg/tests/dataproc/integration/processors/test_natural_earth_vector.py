"""
Unit tests for Natural Earth Vector Processor
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from dataproc import Boundary
from dataproc.processors.core.natural_earth_vector.version_1 import (
    Processor,
    Metadata,
)
from config import get_db_uri_sync, API_DB_NAME
from tests.helpers import (
    load_country_geojson,
    assert_table_in_pg,
    drop_natural_earth_roads_from_pg,
    setup_test_data_paths,
    assert_datapackage_resource,
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
)


class TestNaturalEarthVectorProcessor(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        cls.test_processing_data_dir = os.path.join(
            LOCAL_FS_PROCESSING_DATA_TOP_DIR, "natural_earth_vector"
        )
        os.makedirs(cls.test_processing_data_dir, exist_ok=True)
        cls.test_data_dir = None
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_PACKAGE_DATA_TOP_DIR)

    @classmethod
    def tearDownClass(cls):
        # Cleans processing data
        try:
            # Tmp and Source data
            shutil.rmtree(cls.test_processing_data_dir)
            # Package data
            shutil.rmtree(
                os.path.join(cls.storage_backend.top_level_folder_path, "gambia")
            )
        except FileNotFoundError:
            print("Skipped removing test data tree for", cls.__name__)
        try:
            drop_natural_earth_roads_from_pg()
        except:
            pass

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

    def test_fetch_zip(self):
        """Test the fetching of source zip, unpacking and assertion"""
        zip_fpath = self.proc._fetch_zip()
        self.assertTrue(os.path.exists(zip_fpath))

    def test_fetch_source(self):
        """Test the fetching of source zip, unpacking and assertion"""
        tablename = self.proc._fetch_source()
        assert_table_in_pg(get_db_uri_sync(API_DB_NAME), tablename)

    def test_generate(self):
        """E2E generate test - fetch, crop, push"""
        # Remove the final package artifacts (but keep the test data artifacts if they exist)
        try:
            shutil.rmtree(
                os.path.join(self.storage_backend.top_level_folder_path, "gambia")
            )
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
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
