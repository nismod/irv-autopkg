"""
Unit tests for Dataproc classes
"""
import os
import unittest

from dataproc.backends.storage.localfs import LocalFSStorageBackend
from tests.helpers import create_tree, remove_tree

LOCAL_FS_DATA_TOP_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "packages",
)


class TestLocalFSBackend(unittest.TestCase):
    """"""

    def setUp(self):
        self.backend = LocalFSStorageBackend(LOCAL_FS_DATA_TOP_DIR)


    def expected_fs_structure(self):
        """
        The expected initial FS structure
        """
        return {
            "gambia": {
                "aqueduct": ["0.1"],
                "biodiversity": ["version_1"],
                "osm_roads": ["20221201"],
            },
            "zambia": {"osm_roads": ["20230401"]},
        }

    def test_init(self):
        """Initialisation of the backend and methods available"""
        self.assertIsInstance(self.backend, LocalFSStorageBackend)
        self.assertEqual(self.backend.top_level_folder_path, LOCAL_FS_DATA_TOP_DIR)
        self.assertTrue(
            hasattr(self.backend, "tree") and callable(getattr(self.backend, "tree"))
        )

    def test_tree(self):
        """Test Generation of the package / dataset / version structure"""
        create_tree(LOCAL_FS_DATA_TOP_DIR)
        tree = self.backend.tree()
        self.assertDictEqual(tree, self.expected_fs_structure())
        remove_tree(LOCAL_FS_DATA_TOP_DIR)
