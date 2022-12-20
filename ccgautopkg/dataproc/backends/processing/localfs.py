"""
Local Filesystem Backend
"""

import os
import shutil

from dataproc.exceptions import FolderCreationException, FileCreationException

from ..base import ProcessingBackend


class LocalFSProcessingBackend(ProcessingBackend):
    """Processing Backend for local filesystem"""

    def __init__(self, top_level_folder_path: str) -> None:
        dict.__init__(self)
        self.top_level_folder_path = top_level_folder_path
    