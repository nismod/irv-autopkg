"""
Data Processors
"""
import logging
import inspect
from types import ModuleType
from typing import List

from fastapi import APIRouter, HTTPException

from config import LOG_LEVEL
from dataproc.helpers import list_processors, build_processor_name_version, get_processor_meta_by_name
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

@router.get(PROCESSORS_BASE_ROUTE, response_model=List[schemas.Processor])
async def get_processors():
    """Retrieve information about all available data processors"""
    try:
        results = []
        for proc_name, proc_versions in list_processors().items():
            output_versions = []
            for version in proc_versions:
                meta = get_processor_meta_by_name(
                    build_processor_name_version(proc_name, version)
                )()
                version = schemas.ProcessorVersion(
                    version=version,
                    processor=schemas.ProcessorMetadata(
                        name=build_processor_name_version(meta.name, version),
                        description=meta.description,
                        dataset=meta.dataset_name,
                        author=meta.data_author,
                        license=meta.data_license.asdict(),
                        origin_url=meta.data_origin_url,
                        version=meta.version
                    )
                )
                output_versions.append(version)
            results.append(
                schemas.Processor(
                    name=proc_name,
                    versions=output_versions
                )
            )
        return results
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
    

    
