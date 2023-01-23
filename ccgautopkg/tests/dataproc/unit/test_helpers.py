"""
Unit tests for Dataproc Helpers
"""
import os
import unittest

from dataproc.helpers import add_license_to_datapackage, add_dataset_to_datapackage
from dataproc.backends.storage.localfs import LocalFSStorageBackend
from dataproc.processors.core.test_processor.version_1 import (
    Metadata as TestProcMetadata,
)
from dataproc.processors.internal.base import (
    DataPackageResource,
    DataPackageLicense,
)
from tests.helpers import create_tree, remove_tree

LOCAL_FS_DATA_TOP_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "packages",
)


class TestDataProcHelpers(unittest.TestCase):
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
        to_add = {"name": "test_license"}
        datapackage = add_license_to_datapackage(to_add, self.datapackage)
        self.assertIn(to_add, datapackage["licenses"])
        # Second addition has no effect
        datapackage = add_license_to_datapackage(to_add, self.datapackage)
        self.assertEqual(len(datapackage["licenses"]), 1)

    def test_add_dataset_to_datapackage(self):
        """Test addition of a dataset to a datapackage"""

        expected = {
            "name": TestProcMetadata().name,
            "version": TestProcMetadata().version,
            "path": "/test/uri",
            "description": TestProcMetadata().description,
            "format": "data_format",
            "bytes": 1,
            "hash": "243737210677e8f38a4bc8567c108335",
            "sources": TestProcMetadata().data_author,
            "license": TestProcMetadata().data_license,
            "source": TestProcMetadata().data_origin_url,
        }
        datapackage = add_dataset_to_datapackage(
            TestProcMetadata(),
            expected["path"],
            expected["format"],
            expected["hash"],
            expected["bytes"],
            self.datapackage,
        )
        self.assertIn(expected, datapackage["resources"])
        # Second addition has no effect
        datapackage = add_dataset_to_datapackage(
            TestProcMetadata(),
            expected["path"],
            expected["format"],
            expected["hash"],
            expected["bytes"],
            self.datapackage,
        )
        self.assertEqual(len(datapackage["resources"]), 1)
