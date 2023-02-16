"""
Unit tests for JRC GHSL Population
"""
import os
import unittest
import shutil

from dataproc.backends import LocalFSStorageBackend
from dataproc import Boundary
from dataproc.processors.core.jrc_ghsl_population.r2022_epoch2020_1km import (
    Processor,
    Metadata,
)
from dataproc.helpers import assert_geotiff
from tests.helpers import (
    load_country_geojson,
    assert_raster_bounds_correct,
    assert_datapackage_resource,
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
    DummyTaskExecutor,
)
from config import PACKAGES_HOST_URL


class TestJRCGHSLPopR2022E20201KMProcessor(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        cls.test_processing_data_dir = os.path.join(
            LOCAL_FS_PROCESSING_DATA_TOP_DIR, Metadata().name, Metadata().version
        )
        os.makedirs(cls.test_processing_data_dir, exist_ok=True)
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)
        cls.storage_backend = LocalFSStorageBackend(LOCAL_FS_PACKAGE_DATA_TOP_DIR)

    @classmethod
    def tearDownClass(cls):
        # Tmp and Source data
        shutil.rmtree(cls.test_processing_data_dir, ignore_errors=True)
        # Package data
        shutil.rmtree(
            os.path.join(cls.storage_backend.top_level_folder_path, "gambia"),
            ignore_errors=True,
        )

    def setUp(self):
        self.task_executor = DummyTaskExecutor()
        self.meta = Metadata()
        self.proc = Processor(
            self.meta,
            self.boundary,
            self.storage_backend,
            self.task_executor,
            self.test_processing_data_dir,
        )
        # Change the expected zip URL so we just download the tile over gambia
        self.proc.zip_url = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2022A/GHS_POP_E2020_GLOBE_R2022A_54009_1000/V1-0/tiles/GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0_R8_C17.zip"
        self.proc.source_fnames = [
            "GHS_POP_E2020_GLOBE_R2022A_54009_1000_V1_0_R8_C17.zip"
        ]

    def test_processor_init(self):
        """"""
        self.assertIsInstance(self.proc, Processor)

    def test_context_manager(self):
        """"""
        with Processor(
            self.meta,
            self.boundary,
            self.storage_backend,
            self.task_executor,
            self.test_processing_data_dir,
        ) as proc:
            self.assertIsInstance(proc, Processor)

    def test_context_manager_cleanup_on_error(self):
        """"""
        with Processor(
            self.meta,
            self.boundary,
            self.storage_backend,
            self.task_executor,
            self.test_processing_data_dir,
        ) as proc:
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

    def test_generate(self):
        """E2E generate test - fetch, crop, push"""
        try:
            shutil.rmtree(
                os.path.join(self.storage_backend.top_level_folder_path, "gambia")
            )
        except FileNotFoundError:
            pass
        # Limit the files to be downloaded  in the fetcher
        self.proc.total_expected_files = 1
        prov_log = self.proc.generate()
        # Assert the log contains successful entries
        self.assertTrue(
            prov_log[f"{self.proc.metadata.name} - move to storage success"]
        )
        # Collect the URIs for the final Raster
        final_uri = prov_log[f"{self.proc.metadata.name} - result URI"]
        # # Assert the geotiffs are valid
        assert_geotiff(
            final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
            check_crs="ESRI:54009",
        )
        # # Assert the envelopes - NOTE: this will assert the Molleweide bounds
        assert_raster_bounds_correct(
            final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR),
            self.boundary["envelope_geojson"],
        )
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log["datapackage"])
