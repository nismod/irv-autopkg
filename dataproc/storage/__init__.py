""""""
from config import (
    LOCALFS_STORAGE_BACKEND_ROOT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    S3_REGION,
)
from dataproc.exceptions import ConfigException
from .base import StorageBackend
from .localfs import LocalFSStorageBackend
from .awss3 import AWSS3StorageBackend


def init_storage_backend(storage_backend: str) -> StorageBackend:
    """
    Initialise a StorageBackend by name
    """
    if storage_backend == "localfs":
        return LocalFSStorageBackend(LOCALFS_STORAGE_BACKEND_ROOT)
    elif storage_backend == "awss3":
        return AWSS3StorageBackend(
            S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, s3_region=S3_REGION
        )
    else:
        raise ConfigException(
            f"Unsupported / Unset StorageBackend {storage_backend} - check env"
        )
