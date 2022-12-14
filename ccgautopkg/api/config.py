"""
Global Config
"""

from os import getenv
import sqlalchemy as sa

# DATAPROC VARS
CELERY_BROKER = getenv("CCG_CELERY_BROKER", 'redis://localhost')
CELERY_BACKEND = getenv("CCG_CELERY_BACKEND", 'redis://localhost')


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

DBURI_API = get_db_uri(API_DB_NAME) # For API Usage
DEPLOYMENT_ENV = getenv("CCGAUTOPKG_DEPLOYMENT_ENV", "dev")
