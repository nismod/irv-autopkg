import os


class StorageBackend(dict):
    """StorageBackend must inherit from dict
    to enable json serialisation by kombu"""

    datasets_folder_name = "datasets"
    dataset_data_folder_name = "data"


class PathsHelper:
    """Helpers for management of paths on the processing backend"""

    def __init__(self, top_level_folder_path: str) -> None:
        self.top_level_folder_path = top_level_folder_path

    def build_absolute_path(self, *relative_path_components) -> str:
        """
        Build an absolute path from a relative path, by pre-pending the configured top level directory
        """
        return os.path.join(self.top_level_folder_path, *relative_path_components)
