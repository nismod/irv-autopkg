"""
Test Processor Python environment
"""
import os
import unittest
from subprocess import check_call


class TestProcessorEnv(unittest.TestCase):
    """"""

    def test_imports(self):
        """"""
        import pandas as pd
        import geopandas as gp
        from osgeo import gdal
        import celery
        import shapely
        import pyproj
        import rasterio
        import pyarrow as pa
        import geopandas as gp
        import psycopg2

    def test_commands(self):
        """"""
        self.assertEqual(check_call(["gdalwarp", "--version"]), 0)
        self.assertEqual(
            check_call(["openssl", "sha1", f"{os.path.abspath(__file__)}"]), 0
        )
