"""
Helpers for GHSL JRC Population
"""

import os
import shutil

from dataproc.helpers import download_file, unpack_zip, assert_geotiff
from dataproc.exceptions import DataProcException

class UnexpectedFilesException(DataProcException):
    """Unexpected files encountered during execution"""

class JRCPopFetcher:

    def __init__(
        self
    ):
        """"""

    def fetch_source(self, source_zip_url, target_folder: str, expected_crs: str="ESRI:54009") -> str:
        """
        Fetch source zip file for a given release / resolution.

        Data will be downloaded and unpacked into target_folder

        ::param source_zip_url str Source zip file which contains the raster data
        ::para target_folder str Target folder to download and unzip source into
        ::kwarg expected_crs str Default Molleweide ESRI:54009

        ::returns final_raster_fpath str The local path to the unpacked zip file
        """
        try:
            os.makedirs(target_folder, exist_ok=True)
            # Pull the zip file to the configured processing backend
            zip_fname = os.path.basename(source_zip_url)
            download_zip_fpath = os.path.join(target_folder, zip_fname)
            local_zip_fpath = download_file(
                source_zip_url,
                download_zip_fpath,
            )
            unpack_zip(local_zip_fpath, target_folder)
            # We expect a single raster in the source zip file
            unpacked_files = os.listdir(target_folder)
            if len([i for i in unpacked_files if os.path.splitext(i)[1] == '.tif']) != 1:
                raise UnexpectedFilesException(f"Source zip contained more than a single tif: {unpacked_files}")
            source_tif_fpath = os.path.join(target_folder, unpacked_files[0])
            # Ensure the tif is valid
            assert_geotiff(source_tif_fpath, check_crs=expected_crs, check_compression=False)
            return source_tif_fpath
        except Exception as err:
            if os.path.exists(target_folder):
                shutil.rmtree(target_folder, ignore_errors=True)
            raise err
        finally:
            # Cleanup zip
            if local_zip_fpath and os.path.exists(local_zip_fpath):
                os.remove(local_zip_fpath)
            
