

from dataproc.backends import StorageBackend
from dataproc.exceptions import ConfigException
from .localfs import LocalFSStorageBackend

def init_storage_backend(storage_backend: str) -> StorageBackend:
    """
    Initialise a StorageBackend by name
    """
    if storage_backend == "localfs":
        return LocalFSStorageBackend
    else:
        raise ConfigException(
            f"Unsupported / Unset StorageBackend {storage_backend} - check env"
        )