"""
Probes - Liveness and Readiness
"""
import logging

from fastapi import APIRouter, HTTPException

from api.config import LOG_LEVEL
from api.routes import LIVENESS_ROUTE, READINESS_ROUTE
from api.db import database


router = APIRouter(
    tags=["probes"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)

@router.get(LIVENESS_ROUTE, tags=["probes"])
async def get_liveness():
    """API Liveness Route"""
    return {"status": "alive"}

@router.get(READINESS_ROUTE, tags=["probes"])
async def get_readiness():
    """API Readiness Route, inc. DB check"""
    if await database.execute("SELECT 1"):
        return {"status": "ready"}
    else:
        raise HTTPException(500, detail="not_ready")
