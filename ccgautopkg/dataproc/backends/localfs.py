"""
Local Filesystem Backend
"""

import os
import shutil

from .base import Backend

class LocalFSBackend(Backend):
    """Backend for local filesystem"""

    def __init__(self) -> None:
        dict.__init__(self)

    def create_folder(self, name: str, path: str) -> bool:
        """Create a folder on the backend"""
        full_path = os.path.join(path, name)
        os.mkdir(full_path)
        return os.path.exists(full_path)

    def put_file(self, source_fpath: str, dest_fpath: str) -> bool:
        """Put a file onto the backend"""
        _dest = shutil.copy(source_fpath, dest_fpath)
        return os.path.exists(_dest)

    def folder_exists(self, folder_path: str) -> bool:
        """If a given folder exists"""
        return os.path.exists(folder_path)

    def file_exists(self, file_path: str) -> bool:
        """If a given file exists"""
        return os.path.exists(file_path)
