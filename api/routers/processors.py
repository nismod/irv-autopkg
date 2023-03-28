"""
Data Processors
"""
import logging
from typing import List

from fastapi import APIRouter, HTTPException

from config import LOG_LEVEL, INCLUDE_TEST_PROCESSORS
from dataproc.helpers import (
    list_processors,
    build_processor_name_version,
    get_processor_meta_by_name,
)
from api.routes import (
    PROCESSORS_BASE_ROUTE,
    PROCESSORS_NAME_ROUTE,
    PROCESSORS_VERSION_ROUTE,
)
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
    """Metadata for all available data processors"""
    try:
        results = []
        for proc_name, proc_versions in list_processors(include_test_processors=INCLUDE_TEST_PROCESSORS).items():
            output_versions = []
            for version in proc_versions:
                name_version = build_processor_name_version(proc_name, version)
                meta = get_processor_meta_by_name(name_version)()
                version = schemas.ProcessorVersionMetadata(
                    name=name_version,
                    description=meta.description,
                    version=meta.version,
                    data_author=meta.data_author,
                    data_title=meta.data_title,
                    data_title_long=meta.data_title_long,
                    data_summary=meta.data_summary,
                    data_citation=meta.data_citation,
                    data_license=meta.data_license.asdict(),
                    data_origin_url=meta.data_origin_url,
                    data_formats=meta.data_formats
                )
                output_versions.append(version)
            results.append(schemas.Processor(name=proc_name, versions=output_versions))
        return results
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(
    PROCESSORS_NAME_ROUTE,
    response_model=schemas.Processor,
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
)
async def get_processor(name: str):
    """Metadata for all versions of a single processor"""
    try:
        for proc_name, proc_versions in list_processors(include_test_processors=INCLUDE_TEST_PROCESSORS).items():
            output_versions = []
            if proc_name == name:
                for version in proc_versions:
                    name_version = build_processor_name_version(name, version)
                    meta = get_processor_meta_by_name(name_version)()
                    version = schemas.ProcessorVersionMetadata(
                        name=name_version,
                        description=meta.description,
                        version=meta.version,
                        data_author=meta.data_author,
                        data_title=meta.data_title,
                        data_title_long=meta.data_title_long,
                        data_summary=meta.data_summary,
                        data_citation=meta.data_citation,
                        data_license=meta.data_license.asdict(),
                        data_origin_url=meta.data_origin_url,
                        data_formats=meta.data_formats
                    )
                    output_versions.append(version)
                return schemas.Processor(name=proc_name, versions=output_versions)
        raise HTTPException(status_code=404, detail=f"no such processor: {name}")
    except HTTPException:
        raise
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(
    PROCESSORS_VERSION_ROUTE,
    response_model=schemas.ProcessorVersionMetadata,
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
)
async def get_processor_version(name: str, version: str):
    """Metadata for a single version of a processor"""
    try:
        name_version = build_processor_name_version(name, version)
        try:
            meta = get_processor_meta_by_name(name_version)()
        except:
            raise HTTPException(
                status_code=404, detail=f"no such processor version: {name_version}"
            )
        return schemas.ProcessorVersionMetadata(
            name=name_version,
            description=meta.description,
            version=meta.version,
            data_author=meta.data_author,
            data_title=meta.data_title,
            data_title_long=meta.data_title_long,
            data_summary=meta.data_summary,
            data_citation=meta.data_citation,
            data_license=meta.data_license.asdict(),
            data_origin_url=meta.data_origin_url,
            data_formats=meta.data_formats
        )
    except HTTPException:
        raise
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
