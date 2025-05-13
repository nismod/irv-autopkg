"""
Package Detail endpoints
"""

from copy import deepcopy
import logging
from typing import List
import inspect

from fastapi import APIRouter, HTTPException

from config import (
    LOG_LEVEL,
    STORAGE_BACKEND,
    PACKAGES_HOST_URL,
    PROCESSORS,
)
from dataproc.exceptions import (
    PackageNotFoundException,
)
from dataproc.backends.storage import init_storage_backend
from api.routes import PACKAGES_BASE_ROUTE, PACKAGE_ROUTE
from api.helpers import (
    handle_exception,
    build_package_url,
)
from api.schemas import (
    Package,
    PackageSummary,
)

router = APIRouter(
    tags=["packages"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)

# Initialise the storage backend helpers
storage_backend = init_storage_backend(STORAGE_BACKEND)


@router.get(PACKAGES_BASE_ROUTE, response_model=List[PackageSummary])
async def get_packages():
    """Retrieve information on available top-level packages (which are created from boundaries)"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        packages = sorted(storage_backend.packages())
        logger.debug("found packages in backend: %s", packages)
        result = []
        for boundary_name in packages:
            result.append(
                PackageSummary(
                    boundary_name=boundary_name,
                    uri=build_package_url(PACKAGES_HOST_URL, boundary_name),
                )
            )
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(PACKAGE_ROUTE, response_model=Package)
async def get_package(boundary_name: str):
    """
    Retrieve information about a specific package (which has been created from a given boundary)
    """
    boundary_name = boundary_name.upper()
    try:
        logger.debug("performing %s", inspect.stack()[0][3])

        # Collect the datapackage from FS
        datapackage = None
        try:
            datapackage = storage_backend.load_datapackage(boundary_name)
            datapkg_resource_names = set()
            for r in datapackage["resources"]:
                datapkg_resource_names.add(f"{r['name']}{r['version']}")
                r["bytes"] = sum(r["bytes"])

            processors_with_status = []
            for processor in PROCESSORS:
                processor_with_status = deepcopy(processor)
                for i, version in enumerate(processor["versions"]):
                    if version["name"] in datapkg_resource_names:
                        processor["versions"][i]["status"] = "complete"
                    else:
                        processor["versions"][i]["status"] = "incomplete"
                processors_with_status.append(processor_with_status)
        except Exception as err:
            handle_exception(logger, err)

        # Return combined info about executing and/or existing datasets for this package
        result = Package(
            boundary_name=boundary_name,
            uri=build_package_url(PACKAGES_HOST_URL, boundary_name),
            processors=processors_with_status,
            datapackage=datapackage if datapackage else {},
        )
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except PackageNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=404, detail=f"Package {boundary_name} not found"
        )
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
