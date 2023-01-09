"""
Global Config
"""

from os import getenv
import logging

import sqlalchemy as sa
from celery import Celery


# DATAPROC VARS
CELERY_BROKER = getenv("CCGAUTOPKG_CELERY_BROKER", "redis://localhost")
CELERY_BACKEND = getenv("CCGAUTOPKG_CELERY_BACKEND", "redis://localhost")
CELERY_APP = Celery("CCG-AutoPackage")
CELERY_APP.conf.broker_url = CELERY_BROKER
CELERY_APP.conf.result_backend = CELERY_BACKEND


# API VARS
MAINTENANCE_DB_NAME = getenv("CCGAUTOPKG_MAINTENANCE_DB", "postgres")
API_DB_NAME = getenv("CCGAUTOPKG_POSTGRES_API_DB", "ccgautopkg")


def get_db_uri(dbname: str) -> sa.engine.URL:
    """Standard user DBURI"""
    return sa.engine.URL.create(
        drivername="postgresql+asyncpg",
        username=getenv("CCGAUTOPKG_POSTGRES_USER"),
        password=getenv("CCGAUTOPKG_POSTGRES_PASSWORD"),
        host=getenv("CCGAUTOPKG_POSTGRES_HOST"),
        port=getenv("CCGAUTOPKG_POSTGRES_PORT"),
        database=dbname,
    )


def get_db_uri_sync(dbname: str) -> sa.engine.URL:
    """Standard user DBURI - non-async"""
    return sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        username=getenv("CCGAUTOPKG_POSTGRES_USER"),
        password=getenv("CCGAUTOPKG_POSTGRES_PASSWORD"),
        host=getenv("CCGAUTOPKG_POSTGRES_HOST"),
        port=getenv("CCGAUTOPKG_POSTGRES_PORT"),
        database=dbname,
    )


DBURI_API = get_db_uri(API_DB_NAME)  # For API Usage
DEPLOYMENT_ENV = getenv("CCGAUTOPKG_DEPLOYMENT_ENV", "dev")
LOG_LEVEL = logging.getLevelName(getenv("CCGAUTOPKG_LOG_LEVEL", "DEBUG"))
INTEGRATION_TEST_ENDPOINT = "http://localhost:8000"

# Storage backend to use
STORAGE_BACKEND = getenv("CCGAUTOPKG_STORAGE_BACKEND", "localfs")
# The root-level folder when using localfs storage backend
LOCALFS_STORAGE_BACKEND_ROOT = getenv(
    "CCGAUTOPKG_LOCALFS_STORAGE_BACKEND_ROOT",
    "/Users/dusted/Documents/code/oxford/gri-autopkg/data/package_bucket",
)
# Processing backend to use
PROCESSING_BACKEND = getenv("CCGAUTOPKG_PROCESSING_BACKEND", "localfs")
# The root-level folder when using localfs processing backend
LOCALFS_PROCESSING_BACKEND_ROOT = getenv(
    "CCGAUTOPKG_LOCALFS_PROCESSING_BACKEND_ROOT",
    "/Users/dusted/Documents/code/oxford/gri-autopkg/data/tmp",
)

# Name matching Soundex Distance Default
NAME_SEARCH_DISTANCE = int(getenv("CCGAUTOPKG_NAME_SEARCH_DISTANCE", "2"))
