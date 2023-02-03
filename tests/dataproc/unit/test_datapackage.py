"""
Unit tests for Datapackage Generation
"""
import os
import unittest

from dataproc.helpers import add_license_to_datapackage, add_dataset_to_datapackage
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
            "profile": "ghana-data-package",
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
        expected = DataPackageResource(
            name=TestProcMetadata().name,
            version=TestProcMetadata().version,
            path="/test/uri",
            description=TestProcMetadata().description,
            dataset_format="data_format",
            dataset_size_bytes=1,
            dataset_hashes=[{"/test/uri": "243737210677e8f38a4bc8567c108335"}],
            sources=[TestProcMetadata().data_author],
            dp_license=TestProcMetadata().data_license,
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
