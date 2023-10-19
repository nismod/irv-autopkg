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
) -> str:
    """Standard user DBURI"""
    return str(
        sa.engine.URL.create(
            drivername="postgresql+asyncpg",
            username=getenv(username_env),
            password=getenv(password_env),
            host=getenv(host_env),
            port=getenv(port_env),
            database=dbname,
        )
    )


def get_db_uri_ogr(
    dbname: str,
    username_env="AUTOPKG_POSTGRES_USER",
    password_env="AUTOPKG_POSTGRES_PASSWORD",
    host_env="AUTOPKG_POSTGRES_HOST",
    port_env="AUTOPKG_POSTGRES_PORT",
) -> str:
    """Standard user DBURI for use with OGR (no psycopg2)"""
    for var in [username_env, password_env, host_env, port_env]:
        if not getenv(var):
            raise Exception(f"Environment failed to parse - check var: {var}")
    return str(
        sa.engine.URL.create(
            drivername="postgresql",
            username=getenv(username_env),
            password=getenv(password_env),
            host=getenv(host_env),
            port=getenv(port_env),
            database=dbname,
        )
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
DEPLOYMENT_ENV = getenv("AUTOPKG_DEPLOYMENT_ENV", "prod")
LOG_LEVEL = logging.getLevelName(getenv("AUTOPKG_LOG_LEVEL", "DEBUG"))
INTEGRATION_TEST_ENDPOINT = getenv(
    "AUTOPKG_INTEGRATION_TEST_ENDPOINT", "http://localhost:8000"
)

# Celery Env
CELERY_BROKER = getenv("AUTOPKG_CELERY_BROKER", "redis://localhost")
CELERY_BACKEND = getenv("AUTOPKG_CELERY_BACKEND", "redis://localhost")
CELERY_CONCURRENCY = int(getenv("AUTOPKG_CELERY_CONCURRENCY", "2"))
REDIS_HOST = getenv("AUTOPKG_REDIS_HOST", "localhost")
TASK_LOCK_TIMEOUT = int(getenv("AUTOPKG_TASK_LOCK_TIMEOUT", "600"))

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
if DEPLOYMENT_ENV == "test":
    # TEST
    # The root-level folder when using localfs storage backend
    LOCALFS_STORAGE_BACKEND_ROOT = getenv(
        "AUTOPKG_LOCALFS_STORAGE_BACKEND_ROOT_TEST",
        path.join(path.dirname(path.abspath(__file__)), "tests", "data", "packages"),
    )
    # The root-level folder when using localfs processing backend
    LOCALFS_PROCESSING_BACKEND_ROOT = getenv(
        "AUTOPKG_LOCALFS_PROCESSING_BACKEND_ROOT_TEST",
        path.join(path.dirname(path.abspath(__file__)), "tests", "data", "processing"),
    )
    # Integration tests which require access to the GRIOSM Postgres instance will be run if this is set-True (1)
    TEST_GRI_OSM = True if getenv("AUTOPKG_TEST_GRI_OSM", "True") == "True" else False
    # AWSS3 Storage Backend
    S3_ACCESS_KEY = getenv("AUTOPKG_S3_TEST_ACCESS_KEY")
    S3_SECRET_KEY = getenv("AUTOPKG_S3_TEST_SECRET_KEY")
    # Top level S3 bucket, under-which packages are stored if using AWSS3 backend
    S3_BUCKET = getenv("AUTOPKG_S3_TEST_BUCKET", "irv-autopkg-dev")
    S3_REGION = getenv("AUTOPKG_S3_REGION", "eu-west-2")
else:
    # PROD
    # The root-level folder when using localfs storage backend
    LOCALFS_STORAGE_BACKEND_ROOT = getenv("AUTOPKG_LOCALFS_STORAGE_BACKEND_ROOT")
    # The root-level folder when using localfs processing backend
    LOCALFS_PROCESSING_BACKEND_ROOT = getenv("AUTOPKG_LOCALFS_PROCESSING_BACKEND_ROOT")
    # AWSS3 Storage Backend
    S3_ACCESS_KEY = getenv("AUTOPKG_S3_ACCESS_KEY")
    S3_SECRET_KEY = getenv("AUTOPKG_S3_SECRET_KEY")
    # Top level S3 bucket, under-which packages are stored if using AWSS3 backend
    S3_BUCKET = getenv("AUTOPKG_S3_BUCKET", "irv-autopkg")
    S3_REGION = getenv("AUTOPKG_S3_REGION", "eu-west-2")

# Initialised Startup Data
DBURI_API = get_db_uri(API_POSTGRES_DB)
CELERY_APP = Celery(
    "AutoPackage",
    worker_prefetch_multiplier=1,  # Do not change - long running tasks require this. See: https://docs.celeryq.dev/en/stable/userguide/configuration.html#std-setting-worker_prefetch_multiplier
    worker_concurrency=CELERY_CONCURRENCY,
    broker_url=CELERY_BROKER,
    result_backend=CELERY_BACKEND,
    result_extended=True,
)

# Seconds before submitted tasks expire
TASK_EXPIRY_SECS = int(getenv("AUTOPKG_TASK_EXPIRY_SECS", "3600"))

# Remove Test Processors from the available processors list
INCLUDE_TEST_PROCESSORS = (
    True if getenv("AUTOPKG_INCLUDE_TEST_PROCESSORS", "True") == "True" else False
)
