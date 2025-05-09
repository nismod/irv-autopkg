"""
FastAPI App Main
"""

import logging
import time

from fastapi import FastAPI
import uvicorn

from api.db import database
from config import (
    DEPLOYMENT_ENV,
    LOG_LEVEL,
    LOCALFS_STORAGE_BACKEND_ROOT,
    LOCALFS_PROCESSING_BACKEND_ROOT,
)

from api.routers import jobs, packages, probes, boundaries, processors

OPENAPI_TAGS_META = [
    {
        "name": "boundaries",
        "description": "Detail about boundaries available for generating packages against",
    },
    {
        "name": "packages",
        "description": "Detail about existing packages",
    },
]

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
    try:
        await database.connect()
        logger.info("Connected to Postgres - Success")
    except:
        time.sleep(10)
        await database.connect()
        logger.info("Connected to Postgres - Success")


@app.on_event("shutdown")
async def shutdown():
    """Shutdown hooks"""
    await database.disconnect()


# Routers
app.include_router(packages.router)
app.include_router(boundaries.router)
