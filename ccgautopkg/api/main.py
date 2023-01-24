"""
FastAPI App Main
"""

import logging

from fastapi import FastAPI
import uvicorn

from api.db import database
from config import (
    DEPLOYMENT_ENV,
    LOG_LEVEL,
    CELERY_APP,
    LOCALFS_STORAGE_BACKEND_ROOT,
    LOCALFS_PROCESSING_BACKEND_ROOT,
)

from api.routers import jobs, packages, probes, boundaries, processors
from api.helpers import OPENAPI_TAGS_META

app = FastAPI(
    debug=True if DEPLOYMENT_ENV == "dev" else False, openapi_tags=OPENAPI_TAGS_META
)


@app.on_event("startup")
async def startup():
    """Startup hooks"""
    logger = logging.getLogger("uvicorn.access")
    logger.setLevel(LOG_LEVEL)
    console_formatter = uvicorn.logging.ColourizedFormatter(
        "{asctime} {levelprefix} {pathname} : {lineno}: {message}",
        style="{",
        use_colors=True,
    )
    logger.handlers[0].setFormatter(console_formatter)
    logger.info(
        "Booting API with env: %s, package backend: %s, processing backend: %s",
        DEPLOYMENT_ENV,
        LOCALFS_STORAGE_BACKEND_ROOT,
        LOCALFS_PROCESSING_BACKEND_ROOT,
    )
    await database.connect()
    logger.info("Connected to Postgres - Success")
    # Test connection to Celery
    try:
        _ = CELERY_APP.backend.get_result("noexist")
        logger.info("Connected to Celery with backend - Success")
    except Exception as err:
        print(
            f"WARNING - Failed to connect to Celery backend at: {CELERY_APP._get_backend()}, {err}"
        )


@app.on_event("shutdown")
async def shutdown():
    """Shutdown hooks"""
    await database.disconnect()


# Routers
app.include_router(probes.router)
app.include_router(jobs.router)
app.include_router(packages.router)
app.include_router(boundaries.router)
app.include_router(processors.router)
