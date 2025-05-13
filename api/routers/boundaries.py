"""
Boundary LIST endpoints
"""

import inspect
from typing import List
import logging

from fastapi import APIRouter, HTTPException

import api.db.controller as controller

from config import LOG_LEVEL
from api import schemas
from api.db.database import SessionDep
from api.exceptions import BoundaryNotFoundException
from api.helpers import handle_exception
from api.routes import BOUNDARIES_BASE_ROUTE, BOUNDARY_ROUTE

router = APIRouter(
    tags=["boundaries"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)


@router.get(BOUNDARIES_BASE_ROUTE, response_model=List[schemas.BoundarySummary])
async def get_all_boundary_summaries(session: SessionDep):
    """Retrieve summary information on available boundaries"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        result = controller.get_all_boundary_summaries(session)
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(BOUNDARY_ROUTE, response_model=schemas.Boundary)
async def get_boundary_by_name(name: str, session: SessionDep):
    """Retrieved detailed information on a specific boundary"""
    try:
        logger.debug("performing %s with name %s", inspect.stack()[0][3], name)
        result = controller.get_boundary_by_name(name, session)
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except BoundaryNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=404, detail=f"Requested boundary {name} could not be found"
        )
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
