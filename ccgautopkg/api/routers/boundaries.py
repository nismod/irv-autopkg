"""
Boundary LIST endpoints
"""

from fastapi import APIRouter, HTTPException
from fastapi.logger import logger

from api.config import LOG_LEVEL
from api.routes import BOUNDARIES_BASE_ROUTE
from api.db import database
from api.helpers import handle_exception


logger.setLevel(LOG_LEVEL)
router = APIRouter(
    tags=["boundaries"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

@router.get(BOUNDARIES_BASE_ROUTE)
async def get_boundaries():
    """Retrieve information on available boundaries"""
    try:
        raise NotImplementedError()
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)

