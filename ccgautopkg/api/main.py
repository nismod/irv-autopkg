"""
FastAPI App Main
"""

from fastapi import FastAPI
from fastapi.logger import logger

from api.db import database
from api.config import DEPLOYMENT_ENV, LOG_LEVEL

from api.routers import jobs, packages, probes, boundaries
from api.helpers import OPENAPI_TAGS_META

app = FastAPI(
    debug=True if DEPLOYMENT_ENV == "dev" else False,
    openapi_tags=OPENAPI_TAGS_META
)
logger.setLevel(LOG_LEVEL)


@app.on_event("startup")
async def startup():
    """Startup hooks"""
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    """Shutdown hooks"""
    await database.disconnect()


# Routers
app.include_router(probes.router)
app.include_router(jobs.router)
app.include_router(packages.router)
app.include_router(boundaries.router)
