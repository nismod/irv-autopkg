"""
Probes - Liveness and Readiness
"""
import logging

from fastapi import APIRouter, HTTPException

from config import LOG_LEVEL
from api.routes import LIVENESS_ROUTE, READINESS_ROUTE
from api.db import database
from api.helpers import get_celery_active_tasks


ROUTER = APIRouter(
    tags=["probes"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
LOGGER = logging.getLogger("uvicorn.access")
LOGGER.setLevel(LOG_LEVEL)


@ROUTER.get(LIVENESS_ROUTE, tags=["probes"])
async def get_liveness():
    """API Liveness Route"""
    return {"status": "alive"}


@ROUTER.get(READINESS_ROUTE, tags=["probes"])
async def get_readiness():
    """API Readiness Route, inc. DB check"""
    readiness = []
    if await database.execute("SELECT 1"):
        readiness.append(True)
    else:
        readiness.append(False)
    # Check celery is contactable
    if get_celery_active_tasks() is not None:
        readiness.append(True)
    else:
        readiness.append(False)
    if all(readiness):
        return {"status": "ready"}
    else:
        raise HTTPException(500, detail="not_ready")
