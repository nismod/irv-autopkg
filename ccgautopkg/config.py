"""
Global Config
"""

from os import getenv, path
import logging

import sqlalchemy as sa
from celery import Celery

def get_db_uri(
    dbname: str,
    username_env="CCGAUTOPKG_POSTGRES_USER",
    password_env="CCGAUTOPKG_POSTGRES_PASSWORD",
    host_env="CCGAUTOPKG_POSTGRES_HOST",
    port_env="CCGAUTOPKG_POSTGRES_PORT",
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


def get_db_uri_sync(
    dbname: str,
    username_env="CCGAUTOPKG_POSTGRES_USER",
    password_env="CCGAUTOPKG_POSTGRES_PASSWORD",
    host_env="CCGAUTOPKG_POSTGRES_HOST",
    port_env="CCGAUTOPKG_POSTGRES_PORT",
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
REDIS_HOST = getenv("CCGAUTOPKG_REDIS_HOST", "localhost")
TASK_LOCK_TIMEOUT = int(
    getenv("CCGAUTOPKG_TASK_LOCK_TIMEOUT", "600")
)  # seconds before locked tasks timeout

# Deployment Env
DEPLOYMENT_ENV = getenv("CCGAUTOPKG_DEPLOYMENT_ENV", "dev")
LOG_LEVEL = logging.getLevelName(getenv("CCGAUTOPKG_LOG_LEVEL", "DEBUG"))
INTEGRATION_TEST_ENDPOINT = getenv(
    "CCGAUTOPKG_INTEGRATION_TEST_ENDPOINT", "http://localhost:8000"
)

# Celery Env
CELERY_BROKER = getenv("CCGAUTOPKG_CELERY_BROKER", "redis://localhost")
CELERY_BACKEND = getenv("CCGAUTOPKG_CELERY_BACKEND", "redis://localhost")
REDIS_HOST = getenv("CCGAUTOPKG_REDIS_HOST", "localhost")
TASK_LOCK_TIMEOUT = getenv("CCGAUTOPKG_TASK_LOCK_TIMEOUT", "600")

# Api Env
API_POSTGRES_USER = getenv("CCGAUTOPKG_POSTGRES_USER")
API_POSTGRES_PASSWORD = getenv("CCGAUTOPKG_POSTGRES_PASSWORD")
API_POSTGRES_HOST = getenv("CCGAUTOPKG_POSTGRES_HOST")
API_POSTGRES_PORT = getenv("CCGAUTOPKG_POSTGRES_PORT")
API_POSTGRES_DB = getenv("CCGAUTOPKG_POSTGRES_DB", "ccgautopkg")

# Packages URL under-which all packages are served
PACKAGES_HOST_URL = getenv("CCGAUTOPKG_PACKAGES_HOST_URL", "http://localhost/packages")

# Storage backend to use
STORAGE_BACKEND = getenv("CCGAUTOPKG_STORAGE_BACKEND", "localfs")
# Dev / Prod switch for testing
if getenv("CCGAUTOPKG_DEPLOYMENT_ENV", "prod") == "test":
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

# Initialised Startup Data
DBURI_API = get_db_uri(API_POSTGRES_DB)
CELERY_APP = Celery(
    "CCG-AutoPackage",
    worker_prefetch_multiplier=1,
    broker_url=CELERY_BROKER,
    result_backend=CELERY_BACKEND,
)  # see: https://docs.celeryq.dev/en/stable/userguide/configuration.html#std-setting-worker_prefetch_multiplier
