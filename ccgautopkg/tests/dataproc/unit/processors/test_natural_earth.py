"""
Unit tests for Natural Earth Processor
"""
import os
import unittest
import shutil

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from dataproc.backends.processing.localfs import LocalFSProcessingBackend
from tests.helpers import load_country_geojson
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
        os.mkdir(cls.test_data_dir)
        print ("test data dir:", cls.test_data_dir)
        gambia_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson)
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_DATA_TOP_DIR)
        cls.processing_backend = LocalFSProcessingBackend(LOCAL_FS_DATA_TOP_DIR)
    
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_data_dir)

    def setUp(self):
        pass

    def test_init(self):
        pass
