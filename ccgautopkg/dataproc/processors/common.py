"""
Common Methods for Processors
"""

import os

import rasterio
import shapely
from dataproc import Boundary

def assert_geotiff(fpath: str, check_crs: str='EPSG:4326'):
    """
    Check a given file is a valid geotiff
    """
    with rasterio.open(fpath) as src:
        assert src.meta['crs'] == check_crs, f"raster CRS {src.meta['crs']} doesnt not match expected {check_crs}"

def crop_raster(raster_input_fpath: str, raster_output_fpath: str, boundary: Boundary) -> bool:
    """
    Crop a raster file to the given boundary

    Generates a geotiff
    """
    shape = shapely.from_geojson(boundary['geojson'])
    with rasterio.open(raster_input_fpath) as src:
        out_image, out_transform = rasterio.mask.mask(src, shape, crop=True)
        out_meta = src.meta

    out_meta.update({"driver": "GTiff",
                 "height": out_image.shape[1],
                 "width": out_image.shape[2],
                 "transform": out_transform})

    with rasterio.open(raster_output_fpath, "w", **out_meta) as dest:
        dest.write(out_image)
    
    return os.path.exists(raster_output_fpath)