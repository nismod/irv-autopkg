"""
Unit tests for JRC GHSL Built C
"""
import os
import unittest
import shutil

from dataproc.backends import LocalFSStorageBackend
from dataproc import Boundary
from dataproc.processors.core.jrc_ghsl_built_c.r2022_epoch2018_10m_mszfun import (
    Processor,
    Metadata,
)
from dataproc.helpers import assert_geotiff
from tests.helpers import (
    load_country_geojson,
    assert_raster_bounds_correct,
    setup_test_data_paths,
    assert_datapackage_resource
)
from tests.dataproc.integration.processors import (
    LOCAL_FS_PROCESSING_DATA_TOP_DIR,
    LOCAL_FS_PACKAGE_DATA_TOP_DIR,
)
from config import PACKAGES_HOST_URL


class TestJRCGHSLBuiltCR2022Processor(unittest.TestCase):
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
        shutil.rmtree(cls.test_processing_data_dir)
        # Package data
        shutil.rmtree(os.path.join(cls.storage_backend.top_level_folder_path, "gambia"))

    def setUp(self):
        self.proc = Processor(self.boundary, self.storage_backend)
        # Change the expected zip URLs so we just download the tile over gambia
        self.proc.fetcher.msz_source_url = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_C_GLOBE_R2022A/GHS_BUILT_C_MSZ_GLOBE_R2022A/GHS_BUILT_C_MSZ_E2018_GLOBE_R2022A_54009_10/V1-0/tiles/GHS_BUILT_C_MSZ_E2018_GLOBE_R2022A_54009_10_V1_0_R8_C17.zip"
        self.proc.fetcher.fun_source_url = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_C_GLOBE_R2022A/GHS_BUILT_C_FUN_GLOBE_R2022A/GHS_BUILT_C_FUN_E2018_GLOBE_R2022A_54009_10/V1-0/tiles/GHS_BUILT_C_FUN_E2018_GLOBE_R2022A_54009_10_V1_0_R8_C17.zip"
        self.proc.fetcher.msz_expected_hash = '19906b4e60417bb142d044e68ab7fb3155a63a08'
        self.proc.fetcher.fun_expected_hash = '0bb2743e9dd3b414e7307a9023a0b717ee4f4dff'
        # __NOTE__: Reset the paths helper to reflect the test environment for processing root
        setup_test_data_paths(self.proc, self.test_processing_data_dir)
        self.meta = Metadata()

    def test_processor_init(self):
        """"""
        self.assertIsInstance(self.proc, Processor)

    def test_context_manager(self):
        """"""
        with Processor(self.boundary, self.storage_backend) as proc:
            self.assertIsInstance(proc, Processor)

    def test_context_manager_cleanup_on_error(self):
        """"""
        with Processor(self.boundary, self.storage_backend) as proc:
            setup_test_data_paths(self.proc, self.test_processing_data_dir)
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
        self.proc.total_expected_files = 2
        prov_log = self.proc.generate()
        # Assert the log contains successful entries
        self.assertTrue(prov_log[f"{Metadata().name} - move to storage success"])
        # Collect the URIs for the final Rasters
        final_uris = prov_log[f"{Metadata().name} - result URIs"]
        self.assertEqual(len(final_uris.split(",")), self.proc.total_expected_files)
        for final_uri in final_uris.split(","):
            # # Assert the geotiffs are valid
            assert_geotiff(final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR), check_crs="ESRI:54009")
            # # Assert the envelopes
            assert_raster_bounds_correct(final_uri.replace(PACKAGES_HOST_URL, LOCAL_FS_PACKAGE_DATA_TOP_DIR), self.boundary["envelope_geojson"])
        # Check the datapackage thats included in the prov log
        self.assertIn("datapackage", prov_log.keys())
        assert_datapackage_resource(prov_log['datapackage'])
