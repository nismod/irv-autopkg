"""
Local Filesystem Backend
"""

import os
import urllib

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
        path = self.build_absolute_path(relative_path.split("/"))
        return os.path.exists(path)

    def create_processing_folder(self, folder_path: str):
        """
        Create a folder, or tree on the processing backend top level
        """
        path = self.build_absolute_path(folder_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return os.path.exists(path)

    def download_file(self, source_url: str, destination_relative_fpath: str) -> str:
        """
        Download a file from a source URL to a given destination on the processing backend,
            which is relative to the processing backend top_level_folder_path.
        
        Folders to the path will be created as required
        """
        path = self.build_absolute_path(destination_relative_fpath.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        urllib.request.urlretrieve(source_url, path)
        if not os.path.exists(path):
            raise FileCreationException()
        else:
            return path