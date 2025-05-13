"""
FastAPI App Main
"""

import logging
import time

from fastapi import FastAPI
import uvicorn

from config import (
    DEPLOYMENT_ENV,
    LOG_LEVEL,
    LOCALFS_STORAGE_BACKEND_ROOT,
    LOCALFS_PROCESSING_BACKEND_ROOT,
)

from api.routers import boundaries, packages, processors

OPENAPI_TAGS_META = [
    {
        "name": "boundaries",
        "description": "Detail about boundaries available for generating packages against",
    },
    {
        "name": "packages",
        "description": "Detail about existing packages",
    },
    {
        "name": "processors",
        "description": "Detail about resources contained in all packages",
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
    formatter = uvicorn.logging.ColourizedFormatter(
        "[{asctime}] {levelname}: {filename} - {funcName} - {message}",
        style="{",
        use_colors=True,
    )
    logger.handlers[0].setFormatter(formatter)
    logger.info(
        "Booting API with env: %s, package backend: %s, processing backend: %s",
        DEPLOYMENT_ENV,
        LOCALFS_STORAGE_BACKEND_ROOT,
        LOCALFS_PROCESSING_BACKEND_ROOT,
    )


# Routers
app.include_router(boundaries.router)
app.include_router(packages.router)
app.include_router(processors.router)
