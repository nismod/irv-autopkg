"""
Global Config
"""

from os import getenv, path
import logging

import sqlalchemy as sa
from celery import Celery

def get_db_uri(
    dbname: str,
    username_env="AUTOPKG_POSTGRES_USER",
    password_env="AUTOPKG_POSTGRES_PASSWORD",
    host_env="AUTOPKG_POSTGRES_HOST",
    port_env="AUTOPKG_POSTGRES_PORT",
) -> sa.engine.URL:
    """Standard user DBURI"""
    return sa.engine.URL.create(
        drivername="postgresql+asyncpg",
        username=getenv(username_env),
        password=getenv(password_env),
        host=getenv(host_env),
        port=getenv(port_env),
        database=dbname,
    )

def get_db_uri_ogr(
    dbname: str,
    username_env="AUTOPKG_POSTGRES_USER",
    password_env="AUTOPKG_POSTGRES_PASSWORD",
    host_env="AUTOPKG_POSTGRES_HOST",
    port_env="AUTOPKG_POSTGRES_PORT",
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


def get_db_uri_sync(
    dbname: str,
    username_env="AUTOPKG_POSTGRES_USER",
    password_env="AUTOPKG_POSTGRES_PASSWORD",
    host_env="AUTOPKG_POSTGRES_HOST",
    port_env="AUTOPKG_POSTGRES_PORT",
    ) -> sa.engine.URL:
    """Standard user DBURI - non-async"""
    return sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        username=getenv(username_env),
        password=getenv(password_env),
        host=getenv(host_env),
        port=getenv(port_env),
        database=dbname,
    )

# DATAPROC VARS
REDIS_HOST = getenv("AUTOPKG_REDIS_HOST", "localhost")
TASK_LOCK_TIMEOUT = int(
    getenv("AUTOPKG_TASK_LOCK_TIMEOUT", "600")
)  # seconds before locked tasks timeout

# Deployment Env
DEPLOYMENT_ENV = getenv("AUTOPKG_DEPLOYMENT_ENV", "dev")
LOG_LEVEL = logging.getLevelName(getenv("AUTOPKG_LOG_LEVEL", "DEBUG"))
INTEGRATION_TEST_ENDPOINT = getenv(
    "AUTOPKG_INTEGRATION_TEST_ENDPOINT", "http://localhost:8000"
)

# Celery Env
CELERY_BROKER = getenv("AUTOPKG_CELERY_BROKER", "redis://localhost")
CELERY_BACKEND = getenv("AUTOPKG_CELERY_BACKEND", "redis://localhost")
REDIS_HOST = getenv("AUTOPKG_REDIS_HOST", "localhost")
TASK_LOCK_TIMEOUT = getenv("AUTOPKG_TASK_LOCK_TIMEOUT", "600")

# Api Env
API_POSTGRES_USER = getenv("AUTOPKG_POSTGRES_USER")
API_POSTGRES_PASSWORD = getenv("AUTOPKG_POSTGRES_PASSWORD")
API_POSTGRES_HOST = getenv("AUTOPKG_POSTGRES_HOST")
API_POSTGRES_PORT = getenv("AUTOPKG_POSTGRES_PORT")
API_POSTGRES_DB = getenv("AUTOPKG_POSTGRES_DB", "ccgautopkg")

# Packages URL under-which all packages are served
PACKAGES_HOST_URL = getenv("AUTOPKG_PACKAGES_HOST_URL", "http://localhost/packages")

# Storage backend to use
STORAGE_BACKEND = getenv("AUTOPKG_STORAGE_BACKEND", "localfs")
# Dev / Prod switch for testing
if getenv("AUTOPKG_DEPLOYMENT_ENV", "prod") == "test":
    # TEST
    # The root-level folder when using localfs storage backend
    LOCALFS_STORAGE_BACKEND_ROOT = getenv("AUTOPKG_LOCALFS_STORAGE_BACKEND_ROOT_TEST", path.join(path.dirname(path.abspath(__file__)), "tests", "data", "packages"))
    # The root-level folder when using localfs processing backend
    LOCALFS_PROCESSING_BACKEND_ROOT = getenv("AUTOPKG_LOCALFS_PROCESSING_BACKEND_ROOT_TEST", path.join(path.dirname(path.abspath(__file__)), "tests", "data", "processing"))
    # Integration tests which require access to the GRIOSM Postgres instance will be run if this is set-True (1)
    TEST_GRI_OSM = bool(int(getenv("TEST_GRI_OSM", "0")))
else:
    # PROD
    # The root-level folder when using localfs storage backend
    LOCALFS_STORAGE_BACKEND_ROOT = getenv("AUTOPKG_LOCALFS_STORAGE_BACKEND_ROOT")
    # The root-level folder when using localfs processing backend
    LOCALFS_PROCESSING_BACKEND_ROOT = getenv("AUTOPKG_LOCALFS_PROCESSING_BACKEND_ROOT")

# Name matching Soundex Distance Default
NAME_SEARCH_DISTANCE = int(getenv("AUTOPKG_NAME_SEARCH_DISTANCE", "2"))

# Initialised Startup Data
DBURI_API = get_db_uri(API_POSTGRES_DB)
CELERY_APP = Celery(
    "AutoPackage",
    worker_prefetch_multiplier=1,
    broker_url=CELERY_BROKER,
    result_backend=CELERY_BACKEND
)  # see: https://docs.celeryq.dev/en/stable/userguide/configuration.html#std-setting-worker_prefetch_multiplier

# Seconds before submitted tasks expire
TASK_EXPIRY_SECS = int(getenv("TASK_EXPIRY_SECS", "3600"))