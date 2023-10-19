"""
Unit tests for Dataproc Helper Methods
"""
import os
import unittest
import shutil

from dataproc import Boundary
from dataproc.helpers import crop_osm_to_geopkg, assert_vector_file
from tests.helpers import load_country_geojson
from tests.dataproc.integration.processors import LOCAL_FS_PROCESSING_DATA_TOP_DIR
from config import TEST_GRI_OSM, get_db_uri_ogr


class TestDataprocHelpers(unittest.TestCase):
    """"""

    @classmethod
    def setUpClass(cls):
        if LOCAL_FS_PROCESSING_DATA_TOP_DIR is None:
            raise KeyError("LOCAL_FS_PROCESSING_DATA_TOP_DIR not found in config")
        cls.test_processing_data_dir = os.path.join(
            LOCAL_FS_PROCESSING_DATA_TOP_DIR, "test_dataproc_helpers"
        )
        os.makedirs(cls.test_processing_data_dir, exist_ok=True)
        gambia_geojson, envelope_geojson = load_country_geojson("gambia")
        cls.boundary = Boundary("gambia", gambia_geojson, envelope_geojson)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_processing_data_dir, ignore_errors=True)

    def test_crop_osm_to_geopkg(self):
        """"""
        if TEST_GRI_OSM is False:
            self.skipTest(f"Skipping crop osm test due TEST_GRI_OSM == {TEST_GRI_OSM}")
        dbname = os.getenv("AUTOPKG_OSM_PGDATABASE")
        if dbname is None:
            raise KeyError("AUTOPKG_OSM_PGDATABASE not found in environment")
        pg_uri = get_db_uri_ogr(
            dbname,
            username_env="AUTOPKG_OSM_PGUSER",
            password_env="AUTOPKG_OSM_PGPASSWORD",
            host_env="AUTOPKG_OSM_PGHOST",
            port_env="AUTOPKG_OSM_PORT",
        )
        output_gpkg_fpath = os.path.join(
            self.test_processing_data_dir, "test_gambia.gpkg"
        )
        limit = 100
        batch_size = 10
        gen = crop_osm_to_geopkg(
            self.boundary,
            str(pg_uri),
            "features",
            output_gpkg_fpath,
            geometry_column="geom",
            extract_type="clip",
            limit=limit,
            batch_size=batch_size,
        )
        all_progress = []
        progress = next(gen)
        final_progress = None
        while True:
            try:
                all_progress.append(progress)
                progress = next(gen)
            except StopIteration:
                final_progress = progress
                break
        self.assertListEqual(
            [
                (limit, i, i, 0, 0)
                for i in range(batch_size, limit + batch_size, batch_size)
            ],
            all_progress[:-1],
        )
        self.assertTupleEqual(
            (limit, limit, limit, 0, 0),
            final_progress,
        )
        assert_vector_file(
            output_gpkg_fpath,
            expected_shape=(100, 14),
            expected_crs="EPSG:4326",
        )
