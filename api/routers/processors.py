"""
Package Detail endpoints
"""

import logging
from typing import List


from fastapi import APIRouter, HTTPException

from config import LOG_LEVEL, PROCESSORS
from api.routes import PROCESSORS_BASE_ROUTE
from api.helpers import handle_exception
from api.schemas import Processor


router = APIRouter(
    tags=["processors"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)


@router.get(PROCESSORS_BASE_ROUTE, response_model=List[Processor])
async def get_processors():
    """Metadata for all available data processors"""
    return PROCESSORS
