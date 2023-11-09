"""
Global vars for test data
"""

import os
from config import LOCALFS_STORAGE_BACKEND_ROOT, LOCALFS_PROCESSING_BACKEND_ROOT


if LOCALFS_PROCESSING_BACKEND_ROOT is None:
    raise KeyError("LOCALFS_PROCESSING_BACKEND_ROOT not found in config")
LOCAL_FS_PROCESSING_DATA_TOP_DIR: str = LOCALFS_PROCESSING_BACKEND_ROOT
if LOCALFS_STORAGE_BACKEND_ROOT is None:
    raise KeyError("LOCALFS_STORAGE_BACKEND_ROOT not found in config")
LOCAL_FS_PACKAGE_DATA_TOP_DIR: str = LOCALFS_STORAGE_BACKEND_ROOT

# Create test environment as required
os.makedirs(LOCAL_FS_PROCESSING_DATA_TOP_DIR, exist_ok=True)
os.makedirs(LOCAL_FS_PACKAGE_DATA_TOP_DIR, exist_ok=True)


# Dummy Task Executor dor collecting progress
class DummyTaskExecutor:
    progress = []

    def update_state(self, state="", metadata={}):
        self.progress.append({"state": state, "metadata": metadata})
