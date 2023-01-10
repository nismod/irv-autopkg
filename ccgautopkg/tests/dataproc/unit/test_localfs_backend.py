"""
Unit tests for Dataproc classes
"""
import os
import shutil
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

    def create_tree(self):
        """
        Create a tree so we can check reading
        """
        os.makedirs(
            os.path.join(
                LOCAL_FS_DATA_TOP_DIR, "gambia", "datasets", "aqueduct", "0.1"
            ),
            exist_ok=True,
        )
        os.makedirs(
            os.path.join(
                LOCAL_FS_DATA_TOP_DIR, "gambia", "datasets", "biodiversity", "version_1"
            ),
            exist_ok=True,
        )
        os.makedirs(
            os.path.join(
                LOCAL_FS_DATA_TOP_DIR, "gambia", "datasets", "biodiversity", "version_2"
            ),
            exist_ok=True,
        )
        os.makedirs(
            os.path.join(
                LOCAL_FS_DATA_TOP_DIR, "gambia", "datasets", "osm_roads", "20221201"
            ),
            exist_ok=True,
        )
        os.makedirs(
            os.path.join(
                LOCAL_FS_DATA_TOP_DIR, "gambia", "datasets", "osm_roads", "20230401"
            ),
            exist_ok=True,
        )
        os.makedirs(
            os.path.join(
                LOCAL_FS_DATA_TOP_DIR, "zambia", "datasets", "osm_roads", "20230401"
            ),
            exist_ok=True,
        )

    def remove_tree(self):
        """
        Cleanup the test tree
        """
        shutil.rmtree(os.path.join(LOCAL_FS_DATA_TOP_DIR, "gambia"))
        shutil.rmtree(os.path.join(LOCAL_FS_DATA_TOP_DIR, "zambia"))

    def expected_fs_structure(self):
        """
        The expected initial FS structure
        """
        return {
            "gambia": {
                "aqueduct": ["0.1"],
                "biodiversity": ["version_1", "version_2"],
                "osm_roads": ["20230401", "20221201"],
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
        self.create_tree()
        tree = self.backend.tree()
        self.assertDictEqual(tree, self.expected_fs_structure())
        self.remove_tree()
