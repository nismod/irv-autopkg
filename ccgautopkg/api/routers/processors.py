"""
Data Processors
"""
import logging
import inspect
from types import ModuleType
from typing import List

from fastapi import APIRouter, HTTPException

from api.config import LOG_LEVEL
from api.routes import PROCESSORS_BASE_ROUTE
from api.db import database
from api import schemas
from api.helpers import handle_exception


router = APIRouter(
    tags=["processors"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)

@router.get(PROCESSORS_BASE_ROUTE, response_model=List[schemas.ProcessorMetadata])
async def get_processors():
    """Retrieve information about all available data processors"""
    try:
        import dataproc.processors.core as available_processors
        results = []
        # Iterate through Core processors and collect metadata
        for name, processor in inspect.getmembers(available_processors):
            # Skip utility modules
            if name in ['_module', 'pkgutil']:
                continue
            print (name, type(processor))
            if type(processor) == ModuleType:
                # Instantiate metadata
                try:
                    meta = processor.Metadata()
                    results.append(
                        schemas.ProcessorMetadata(
                            processor=meta.processor_name,
                            dataset=meta.dataset_name,
                            author=meta.data_author,
                            license=meta.data_license,
                            origin_url=meta.data_origin_url
                        )
                    )
                except Exception as err:
                    # Handle but continue
                    handle_exception(logger, err)
        return results
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
    

    
