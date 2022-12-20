"""
Package Detail endpoints
"""
import logging
from typing import List
import inspect

from fastapi import APIRouter, HTTPException

from api.config import LOG_LEVEL, STORAGE_BACKEND, LOCALFS_STORAGE_BACKEND_ROOT
from api.routes import PACKAGES_BASE_ROUTE, PACKAGE_ROUTE
from api.helpers import (
    handle_exception,
    init_storage_backend,
    get_processor_by_name,
    processor_name,
)
from api.schemas import (
    Package,
    PackageSummary,
    ProcessorMetadata,
    Dataset,
    ProcessorVersion,
)
from api.db.controller import DBController
from dataproc.exceptions import PackageNotFoundException, DatasetNotFoundException

router = APIRouter(
    tags=["packages"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)

storage_backend = init_storage_backend(STORAGE_BACKEND)(LOCALFS_STORAGE_BACKEND_ROOT)


@router.get(PACKAGES_BASE_ROUTE, response_model=List[PackageSummary])
async def get_packages():
    """Retrieve information on available top-level packages (which are created from boundaries)"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        logger.debug("found pacakges in backend: %s", storage_backend.packages())
        result = []
        for boundary_name in storage_backend.packages():
            result.append(PackageSummary(boundary_name=boundary_name, uri="TODO"))
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(PACKAGE_ROUTE, response_model=Package)
async def get_package(boundary_name: str):
    """Retrieve information a specific package (which has been created from a given boundary)"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        datasets = storage_backend.package_datasets(boundary_name)
        # One to one mapping between dataset.version name and the processor version name that creates it
        logger.debug("found datasets: %s", datasets)
        processors_meta = []
        output_datasets = []
        for dataset in datasets:
            processor_versions = []
            for version in storage_backend.dataset_versions(boundary_name, dataset):
                proc_name = processor_name(dataset, version)
                meta = get_processor_by_name(proc_name).Metadata()
                if meta is not None:
                    processors_meta.append(meta)
                    processor_versions.append(
                        ProcessorVersion(
                            processor=ProcessorMetadata(
                                name=meta.name,
                                description=meta.description,
                                dataset=meta.dataset_name,
                                author=meta.data_author,
                                license=meta.data_license,
                                origin_url=meta.data_origin_url,
                                version=meta.version,
                            ),
                            version=meta.version,
                        )
                    )
                else:
                    logger.warning(
                        "Processor Meta not found for dataset: %s, version: %s",
                        dataset,
                        version,
                    )
            output_datasets.append(Dataset(name=dataset, versions=processor_versions))
        boundary = await DBController().get_boundary_by_name(boundary_name)
        result = Package(
            boundary_name=boundary_name,
            uri="TODO",
            boundary=boundary,
            datasets=output_datasets,
        )
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except PackageNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=404, detail=f"Package not found: {boundary_name}"
        )
    except DatasetNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=500, detail="Internal error finding a dataset for a package"
        )
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
