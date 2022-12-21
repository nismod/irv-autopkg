"""
Package Detail endpoints
"""
import logging
from typing import List
import inspect

from fastapi import APIRouter, HTTPException

from config import LOG_LEVEL, STORAGE_BACKEND, LOCALFS_STORAGE_BACKEND_ROOT
from dataproc.helpers import processor_name, dataset_name_from_processor
from dataproc.exceptions import PackageNotFoundException, DatasetNotFoundException
from dataproc.helpers import init_storage_backend
from api.routes import PACKAGES_BASE_ROUTE, PACKAGE_ROUTE
from api.helpers import handle_exception, currently_executing_processors, processor_meta
from api.schemas import (
    Package,
    PackageSummary,
    Dataset,
)
from api.db.controller import DBController
from api.exceptions import PackageHasNoDatasetsException


router = APIRouter(
    tags=["packages"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)

# Initialise the storage backend helpers
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
    """
    Retrieve information a specific package (which has been created from a given boundary)

    Datasets are either executing (being generated),  completed (exist on the FS), or do not exist.
    """
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        output_datasets = []

        # Check for executing tasks (processor.versions)
        executing_processors = currently_executing_processors(boundary_name)
        logger.debug("found executing processors: %s", executing_processors)
        executing_versions = {}
        for processor_name_version in executing_processors:
            dataset = dataset_name_from_processor(processor_name_version)
            if dataset not in executing_versions.keys():
                executing_versions[dataset] = [
                    processor_meta(processor_name_version, executing=True)
                ]
            else:
                executing_versions[dataset].append(
                    processor_meta(processor_name_version, executing=True)
                )
        # Collate into Datasets for output schema
        for dataset, versions in executing_versions.items():
            output_datasets.append(Dataset(name=dataset, versions=versions))

        # Check for existing datasets
        try:
            existing_datasets = storage_backend.package_datasets(boundary_name)
            # One to one mapping between dataset.version name and the processor version name that creates it
            logger.debug("found existing datasets: %s", existing_datasets)
            for dataset in existing_datasets:
                processor_versions = []
                try:
                    for version in storage_backend.dataset_versions(boundary_name, dataset):
                        proc_name = processor_name(dataset, version)
                        processor_versions.append(processor_meta(proc_name))
                except DatasetNotFoundException:
                    logger.debug("No dataset with the given name was found on the FS: %s", dataset)
                output_datasets.append(Dataset(name=dataset, versions=processor_versions))
        except PackageNotFoundException:
            logger.debug("No package with the given name was found on the FS: %s", boundary_name)
        
        # If there are no executing processors, nor existing datasets then return a 404 for this package
        if not output_datasets:
            raise PackageHasNoDatasetsException(boundary_name)

        # Return combined info about executing and/or existing datasets for this package
        boundary = await DBController().get_boundary_by_name(boundary_name)
        result = Package(
            boundary_name=boundary_name,
            uri="TODO",
            boundary=boundary,
            datasets=output_datasets,
        )
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except PackageHasNoDatasetsException as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=404, detail=f"Package {boundary_name} has no existing or executing datasets")
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
