"""
Package Detail endpoints
"""

from fastapi import APIRouter, HTTPException
from fastapi.logger import logger

from api.config import LOG_LEVEL
from api.routes import PACKAGES_BASE_ROUTE, PACKAGE_ROUTE
from api.helpers import handle_exception


logger.setLevel(LOG_LEVEL)
router = APIRouter(
    tags=["packages"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

@router.get(PACKAGES_BASE_ROUTE)
async def get_packages():
    """Retrieve information on available packages"""
    try:
        raise NotImplementedError()
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)

@router.get(PACKAGE_ROUTE)
async def get_package(package_id: str):
    """Reterieve information a specific package"""
    try:
        raise NotImplementedError()
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)