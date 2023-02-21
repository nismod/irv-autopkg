"""
Unit tests for AWS S3 Storage Backend
"""
import unittest

from dataproc.backends.storage.awss3 import AWSS3StorageBackend, S3Manager
from config import (
    S3_BUCKET,
    TEST_AWSS3,
    S3_ACCESS_KEY_ENV,
    S3_SECRET_KEY_ENV,
)
from tests.helpers import create_tree_awss3, remove_tree_awss3


class TestAWSS3StorageBackend(unittest.TestCase):
    """"""

    def setUp(self):
        self.backend = AWSS3StorageBackend(
            S3_BUCKET, S3_ACCESS_KEY_ENV, S3_SECRET_KEY_ENV
        )

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
        if TEST_AWSS3 is False:
            self.skipTest(f"Skipping AWSS3 due to TEST_AWSS3 == {TEST_AWSS3}")
        self.assertIsInstance(self.backend, AWSS3StorageBackend)
        self.assertEqual(self.backend.bucket, S3_BUCKET)
        self.assertTrue(
            hasattr(self.backend, "tree") and callable(getattr(self.backend, "tree"))
        )
        self.assertTrue(self.backend._check_env())

    def test_tree(self):
        """Test Generation of the package / dataset / version structure"""
        with S3Manager(*self.backend._parse_env(), region=self.backend.s3_region) as s3_fs:
            create_tree_awss3(
                s3_fs,
                S3_BUCKET,
            )
        tree = self.backend.tree()
        self.assertDictEqual(tree, self.expected_fs_structure())
        with S3Manager(*self.backend._parse_env(), region=self.backend.s3_region) as s3_fs:
            remove_tree_awss3(
                s3_fs,
                S3_BUCKET,
            )
