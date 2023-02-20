"""
Unit tests for Datapackage Generation
"""
import os
import unittest

from datapackage import Package, Resource

from dataproc.helpers import add_license_to_datapackage, add_dataset_to_datapackage, datapackage_resource
from dataproc.backends.storage.localfs import LocalFSStorageBackend
from dataproc.processors.core.test_processor.version_1 import (
    Metadata as TestProcMetadata,
)
from dataproc import DataPackageResource

LOCAL_FS_DATA_TOP_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "packages",
)


class TestDataPackage(unittest.TestCase):
    """"""

    def setUp(self):
        self.backend = LocalFSStorageBackend(LOCAL_FS_DATA_TOP_DIR)
        self.datapackage = {
            "name": "ghana",
            "title": "ghana",
            "licenses": [],
            "resources": [],
        }

    def test_add_license_to_datapackage(self):
        """Test addition of a license to the datapackage"""
        to_add = TestProcMetadata().data_license
        datapackage = add_license_to_datapackage(to_add, self.datapackage)
        self.assertIn(to_add.asdict(), datapackage["licenses"])
        # Second addition has no effect
        datapackage = add_license_to_datapackage(to_add, self.datapackage)
        self.assertEqual(len(datapackage["licenses"]), 1)

    def test_add_dataset_to_datapackage(self):
        """Test addition of a dataset to a datapackage"""
        expected = datapackage_resource(
            TestProcMetadata(),
            ["http://test.com/test/uri"],
            "GeoTIFF",
            [1],
            [{"/test/uri": "243737210677e8f38a4bc8567c108335"}],
        )
        datapackage = add_dataset_to_datapackage(
            expected,
            self.datapackage,
        )
        self.assertIn(expected.asdict(), datapackage["resources"])
        # Second addition has no effect
        datapackage = add_dataset_to_datapackage(
            expected,
            self.datapackage,
        )
        self.assertEqual(len(datapackage["resources"]), 1)

        # Test parsing the datapackage
        package = Package(datapackage)
        if not package.valid:
            print (package.errors)
            print (datapackage)
        self.assertTrue(package.valid)
