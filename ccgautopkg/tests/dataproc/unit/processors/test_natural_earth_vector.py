"""
Unit tests for Natural Earth Vector Processor
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from tests.helpers import load_country_geojson, assert_table_in_pg, drop_natural_earth_roads_from_pg
from dataproc import Boundary
from dataproc.processors.core.test_natural_earth_vector.version_1 import (
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


class TestNaturalEarthVectorProcessor(unittest.TestCase):
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
            print ('Skipped removing test data tree for', cls.__name__)
        try:
            drop_natural_earth_roads_from_pg()
            pass
        except:
            pass

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
            shutil.rmtree(os.path.join(self.storage_backend.top_level_folder_path, "gambia"))
        except FileNotFoundError:
            pass
        prov_log = self.proc.generate()
        print (prov_log)
        # # Assert the log contains a succesful entries
        self.assertTrue(prov_log[f"{Metadata().name} - crop completed"])
        self.assertTrue(prov_log[f"{Metadata().name} - move to storage success"])
        # # Collect the URI for the final Raster
        final_uri = prov_log[f"{Metadata().name} - result URI"]
        # Assert the file exists
        self.assertTrue(os.path.exists(final_uri))




# ogr2ogr -f "GPKG" /Users/dusted/Documents/code/oxford/gri-autopkg/data/tmp/test_natural_earth_vector/gambia/test_natural_earth_vector/version_1/outputs/gambia.gpkg PG:"postgresql://test_user:test_password@localhost:5432/ccgautopkg" -sql "SELECT * from ne_10m_roads where st_intersects(st_geomfromgeojson('{'type': 'MultiPolygon', 'coordinates': [[[[-16.713728807, 13.594958604], [-15.62459632, 13.623587348], [-15.398770311, 13.860368761], [-15.081735399, 13.876491808], [-14.687030809, 13.63035696], [-14.376713833, 13.625680243], [-14.046992357, 13.794067898], [-13.844963345, 13.505041612], [-14.277701789, 13.280585029], [-14.712197231, 13.298206692], [-15.141163296, 13.509511624], [-15.511812507, 13.278569648], [-15.691000536, 13.270353095], [-15.931295946, 13.130284125], [-16.841524624, 13.151393948], [-16.713728807, 13.594958604]]]]}'), geometry)"