"""
Test Processor Python environment
"""
import unittest

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