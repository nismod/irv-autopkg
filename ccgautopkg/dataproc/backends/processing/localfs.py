"""
Local Filesystem Backend
"""

import os
from typing import List
import requests
import os
import zipfile
import json

import rasterio
import rasterio.mask
import shapely

from dataproc import Boundary
from dataproc.exceptions import FolderCreationException, FileCreationException

from ..base import ProcessingBackend


class LocalFSProcessingBackend(ProcessingBackend):
    """Processing Backend for local filesystem"""

    def __init__(self, top_level_folder_path: str) -> None:
        dict.__init__(self)
        self.top_level_folder_path = top_level_folder_path

    def build_absolute_path(self, *args) -> str:
        """
        Build an absolute path from a relative path, by pre-pending the configured top level directory
        """
        return os.path.join(self.top_level_folder_path, *args)

    def path_exists(self, relative_path: str) -> bool:
        """
        Check if a file or folder exists at the given relative path
        """
        path = os.path.join(self.top_level_folder_path, relative_path)
        return os.path.exists(path)

    def create_processing_folder(self, path_components: List[str]) -> str:
        """
        Create a folder, or tree relative to the processing backend top level

        ::param folder_path str Relative path to create, e.g.: "some/path/here"
        """
        path = self.build_absolute_path(*path_components)
        os.makedirs(path, exist_ok=True)
        if not os.path.exists(path):
            raise FolderCreationException(path)
        return path

    def download_file(self, source_url: str, destination_relative_fpath: str) -> str:
        """
        Download a file from a source URL to a given destination on the processing backend,
            which is relative to the processing backend top_level_folder_path.

        Folders to the path will be created as required
        """
        path = os.path.join(self.top_level_folder_path, destination_relative_fpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # urllib.request.urlretrieve(source_url, path)
        response = requests.get(
            source_url,
            stream=True,
            headers={
                "Accept": "application/zip",
                "Accept-Encoding": "gzip",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
            },
        )
        with open(path, "wb") as handle:
            for data in response.iter_content(chunk_size=8192):
                handle.write(data)
        if not os.path.exists(path):
            raise FileCreationException()
        else:
            return path

    def assert_geotiff(self, fpath: str, check_crs: str = "EPSG:4326"):
        """
        Check a given file is a valid geotiff

        ::param fpath str Absolute filepath
        """
        with rasterio.open(fpath) as src:
            assert (
                src.meta["crs"] == check_crs
            ), f"raster CRS {src.meta['crs']} doesnt not match expected {check_crs}"

    def crop_raster(
        self, raster_input_fpath: str, raster_output_fpath: str, boundary: Boundary
    ) -> bool:
        """
        Crop a raster file to the given boundary

        Generates a geotiff

        ::param raster_input_fpath str Absolute Filepath of input
        ::param raster_output_fpath str Absolute Filepath of output
        """
        # Create the path to output if it doesnt exist
        os.makedirs(os.path.dirname(raster_output_fpath), exist_ok=True)
        shape = shapely.from_geojson(json.dumps(boundary["envelope_geojson"]))
        with rasterio.open(raster_input_fpath) as src:
            out_image, out_transform = rasterio.mask.mask(src, [shape], crop=True)
            out_meta = src.meta

        out_meta.update(
            {
                "driver": "GTiff",
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
            }
        )

        with rasterio.open(raster_output_fpath, "w", **out_meta) as dest:
            dest.write(out_image)

        return os.path.exists(raster_output_fpath)

    def unpack_zip(self, zip_fpath: str) -> str:
        """
        Unpack a Downloaded Zip

        ::param zip_fpath str Absolute Filepath of input

        ::returns extracted folder path str
        """
        extract_path = os.path.dirname(zip_fpath)
        with zipfile.ZipFile(zip_fpath, "r") as zip_ref:
            zip_ref.extractall(extract_path)
        return os.path.join(extract_path, os.path.splitext(zip_fpath)[0])

    def create_test_file(self, fpath: str):
        """
        Generate a blank test-file
        """
        with open(fpath, 'w') as fptr:
            fptr.write("test\n")
