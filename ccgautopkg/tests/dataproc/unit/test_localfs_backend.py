"""
Unit tests for Dataproc classes
"""
import os
import unittest

from dataproc.backends.storage.localfs import LocalFSStorageBackend

LOCAL_FS_DATA_TOP_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "package_bucket",
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
            "aqueduct": ["version_0.1"],
            "biodiversity": ["version_1", "version_2"],
            "osm_roads": ["version_20221201", "version_20230401"],
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
        tree = self.backend.tree()
        self.assertDictEqual(tree, self.expected_fs_structure())
