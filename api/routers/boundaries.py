"""
Boundary LIST endpoints
"""
import inspect
from typing import List, Optional
import logging

from fastapi import APIRouter, HTTPException

from config import LOG_LEVEL
from api.routes import BOUNDARIES_BASE_ROUTE, BOUNDARY_ROUTE, BOUNDARY_SEARCH_ROUTE
from api.db.controller import DBController
from api.helpers import handle_exception
from api import schemas
from api.exceptions import BoundarySearchException, BoundaryNotFoundException

router = APIRouter(
    tags=["boundaries"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)


@router.get(BOUNDARIES_BASE_ROUTE, response_model=List[schemas.BoundarySummary])
async def get_all_boundary_summaries():
    """Retrieve summary information on available boundaries"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        result = await DBController().get_all_boundary_summaries()
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(BOUNDARY_SEARCH_ROUTE, response_model=List[schemas.BoundarySummary])
async def search_boundary(
    name: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
):
    """Search for boundaries by name or coordinates."""
    try:
        logger.debug(
            "performing %s with query %s",
            inspect.stack()[0][3],
            [name, latitude, longitude],
        )
        if latitude is not None and longitude is not None:
            result = await DBController().search_boundaries_by_coordinates(
                latitude, longitude
            )
        elif name:
            result = await DBController().search_boundaries_by_name(name)
        else:
            raise BoundarySearchException(
                "Search must include name or valid latitude, longitude coordinates"
            )
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except BoundarySearchException as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=400, detail=str(err))
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(BOUNDARY_ROUTE, response_model=schemas.Boundary)
async def get_boundary_by_name(name: str):
    """Retrieved detailed information on a specific boundary"""
    try:
        logger.debug("performing %s with name %s", inspect.stack()[0][3], name)
        result = await DBController().get_boundary_by_name(name)
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
