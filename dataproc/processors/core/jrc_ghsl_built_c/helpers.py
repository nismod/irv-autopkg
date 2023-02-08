"""
Helpers for GHSL JRC Built Characteristics (MSZ & Func)
"""

import os
import shutil
from typing import List

from dataproc.helpers import (
    download_file,
    unpack_and_check_zip,
)


class JRCBuiltCFetcher:
    """
    Fetches JRC GHSL Built-C zip files
    """

    def __init__(
        self,
        msz_source_url: str = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_C_GLOBE_R2022A/GHS_BUILT_C_MSZ_GLOBE_R2022A/GHS_BUILT_C_MSZ_E2018_GLOBE_R2022A_54009_10/V1-0/GHS_BUILT_C_MSZ_E2018_GLOBE_R2022A_54009_10_V1_0.zip",
        fun_source_url: str = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_C_GLOBE_R2022A/GHS_BUILT_C_FUN_GLOBE_R2022A/GHS_BUILT_C_FUN_E2018_GLOBE_R2022A_54009_10/V1-0/GHS_BUILT_C_FUN_E2018_GLOBE_R2022A_54009_10_V1_0.zip",
    ):
        """"""
        self.msz_source_url = msz_source_url
        self.msz_expected_hash = "5bdc5a6036f72d119745c02b940cbbadbb881d66"
        self.fun_source_url = fun_source_url
        self.fun_expected_hash = "344f6d54e3253b3c0a3370e7dbe8da328586ec79"

    def fetch_source(
        self, target_folder: str, expected_crs: str = "ESRI:54009"
    ) -> List[str]:
        """
        Fetch source zip files.

        Data will be downloaded and unpacked into target_folder

        ::param source_zip_url str Source zip file which contains the raster data
        ::para target_folder str Target folder to download and unzip source into
        ::kwarg expected_crs str Default Molleweide ESRI:54009

        ::returns final_raster_fpath str The local path to the unpacked zip file
        """
        try:
            os.makedirs(target_folder, exist_ok=True)
            # Pull the msz zip file - 50gb unpacked
            zip_fname = os.path.basename(self.msz_source_url)
            download_zip_fpath = os.path.join(target_folder, zip_fname)
            local_zip_fpath = download_file(
                self.msz_source_url,
                download_zip_fpath,
            )
            msz_tiffs = unpack_and_check_zip(
                local_zip_fpath,
                target_folder,
                expected_crs,
                num_expected_tifs=1,
                expected_hashes=[self.msz_expected_hash],
            )

            # Pull the fun zip file - 50gb unpacked
            zip_fname = os.path.basename(self.fun_source_url)
            download_zip_fpath = os.path.join(target_folder, zip_fname)
            local_zip_fpath = download_file(
                self.fun_source_url,
                download_zip_fpath,
            )
            fun_tiffs = unpack_and_check_zip(
                local_zip_fpath,
                target_folder,
                expected_crs,
                num_expected_tifs=1,
                expected_hashes=[self.fun_expected_hash],
            )

            return msz_tiffs + fun_tiffs
        except Exception as err:
            if os.path.exists(target_folder):
                shutil.rmtree(target_folder, ignore_errors=True)
            raise err
        finally:
            # Cleanup zip
            if local_zip_fpath and os.path.exists(local_zip_fpath):
                os.remove(local_zip_fpath)
