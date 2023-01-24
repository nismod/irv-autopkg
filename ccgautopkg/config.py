"""
Global Config
"""

from os import getenv, path
import logging

import sqlalchemy as sa
from celery import Celery


# DATAPROC VARS
CELERY_APP = Celery(
    "CCG-AutoPackage",
    worker_prefetch_multiplier=1,
    broker_url=getenv("CCGAUTOPKG_CELERY_BROKER", "redis://localhost"),
    result_backend=getenv("CCGAUTOPKG_CELERY_BACKEND", "redis://localhost"),
)  # see: https://docs.celeryq.dev/en/stable/userguide/configuration.html#std-setting-worker_prefetch_multiplier
REDIS_HOST = getenv("CCGAUTOPKG_REDIS_HOST", "localhost")
TASK_LOCK_TIMEOUT = int(
    getenv("CCGAUTOPKG_TASK_LOCK_TIMEOUT", "600")
)  # seconds before locked tasks timeout


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


def get_db_uri_ogr(
    dbname: str,
    username_env="CCGAUTOPKG_POSTGRES_USER",
    password_env="CCGAUTOPKG_POSTGRES_PASSWORD",
    host_env="CCGAUTOPKG_POSTGRES_HOST",
    port_env="CCGAUTOPKG_POSTGRES_PORT",
) -> sa.engine.URL:
    """Standard user DBURI for use with OGR (no psycopg2)"""
    for var in [username_env, password_env, host_env, port_env]:
        if not getenv(var):
            raise Exception(f"Environment failed to parse - check var: {var}")
    return sa.engine.URL.create(
        drivername="postgresql",
        username=getenv(username_env),
        password=getenv(password_env),
        host=getenv(host_env),
        port=getenv(port_env),
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
INTEGRATION_TEST_ENDPOINT = getenv(
    "CCGAUTOPKG_INTEGRATION_TEST_ENDPOINT", "http://localhost:8000"
)

# Storage backend to use
STORAGE_BACKEND = getenv("CCGAUTOPKG_STORAGE_BACKEND", "localfs")
# Dev / Prod switch for testing
if DEPLOYMENT_ENV == getenv("CCGAUTOPKG_DEPLOYMENT_ENV", "test"):
    # The root-level folder when using localfs storage backend
    LOCALFS_STORAGE_BACKEND_ROOT = getenv("CCGAUTOPKG_LOCALFS_STORAGE_BACKEND_ROOT_TEST", path.join(path.dirname(path.abspath(__file__)), "tests", "data", "packages"))
    # The root-level folder when using localfs processing backend
    LOCALFS_PROCESSING_BACKEND_ROOT = getenv("CCGAUTOPKG_LOCALFS_PROCESSING_BACKEND_ROOT_TEST", path.join(path.dirname(path.abspath(__file__)), "tests", "data", "tmp"))
else:
    # The root-level folder when using localfs storage backend
    LOCALFS_STORAGE_BACKEND_ROOT = getenv("CCGAUTOPKG_LOCALFS_STORAGE_BACKEND_ROOT")
    # The root-level folder when using localfs processing backend
    LOCALFS_PROCESSING_BACKEND_ROOT = getenv("CCGAUTOPKG_LOCALFS_PROCESSING_BACKEND_ROOT")

# Name matching Soundex Distance Default
NAME_SEARCH_DISTANCE = int(getenv("CCGAUTOPKG_NAME_SEARCH_DISTANCE", "2"))
