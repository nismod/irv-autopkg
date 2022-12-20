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
from api.helpers import handle_exception, list_processors, build_processor_name_version, get_processor_by_name


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
                proc_module = get_processor_by_name(
                    build_processor_name_version(proc_name, version)
                )
                meta = proc_module.Metadata()
                version = schemas.ProcessorVersion(
                    version=version,
                    processor=schemas.ProcessorMetadata(
                        name=meta.name,
                        description=meta.description,
                        dataset=meta.dataset_name,
                        author=meta.data_author,
                        license=meta.data_license,
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
        # import dataproc.processors.core as available_processors
        # results = []
        # # Iterate through Core processors and collect metadata
        # for name, processor in inspect.getmembers(available_processors):
        #     # Skip utility modules
        #     if name in ['_module', 'pkgutil']:
        #         continue
        #     # Skip top level modules without metadata
        #     if not hasattr(processor, "Metadata"):
        #         continue
        #     logger.debug("found valid processor module: %s, %s", name, type(processor))
        #     if isinstance(processor, ModuleType):
        #         # Instantiate metadata
        #         try:
        #             meta = processor.Metadata()
        #             results.append(
        #                 schemas.ProcessorMetadata(
        #                     name=meta.name,
        #                     description=meta.description,
        #                     dataset=meta.dataset_name,
        #                     author=meta.data_author,
        #                     license=meta.data_license,
        #                     origin_url=meta.data_origin_url,
        #                     version=meta.version
        #                 )
        #             )
        #         except Exception as err:
        #             # Handle but continue
        #             handle_exception(logger, err)
        return results
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
    

    
