"""
Processing Job Endpoints
"""
import logging
import inspect

from fastapi import APIRouter, HTTPException
from celery.utils import uuid

from config import LOG_LEVEL, CELERY_APP
from dataproc import Boundary as DataProcBoundary
from dataproc.helpers import get_processor_by_name
from api import schemas
from api.helpers import (
    handle_exception,
    create_dag,
    random_task_uuid,
    currently_active_or_reserved_processors
)
from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE
from api.exceptions import (
    BoundaryNotFoundException,
    ProcessorNotFoundException,
    JobNotFoundException,
    ProcessorAlreadyExecutingException,
    CannotGetCeleryTasksInfoException
)
from api.db import DBController

router = APIRouter(
    tags=["jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)


@router.get(JOB_STATUS_ROUTE, response_model=schemas.JobStatus)
def get_status(job_id: str):
    """Get status of a DAG associated with a given package"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        job_status = CELERY_APP.backend.get_status(job_id)
        # Note - non existant tasks that are assumed as pending (i.e. a random UUID)
        job_result = CELERY_APP.backend.get_result(job_id)
        logger.debug("Job Status: %s, Job Result: %s", job_status, job_result)
        if not job_status:
            raise JobNotFoundException(f"{job_id}")
        result = schemas.JobStatus(
            job_id=job_id,
            job_status=str(job_status),
            job_result=job_result
            if isinstance(job_result, dict)
            else {"result": str(job_result)},
        )
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except JobNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=404, detail=f"Job not found: {str(err)}")
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.post(JOBS_BASE_ROUTE, response_model=schemas.SubmittedJob)
async def submit_processing_job(job: schemas.Job):
    """Submit a job for a given package to run a list of dataset-processors"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        # Collect boundary geojson
        boundary_db = await DBController().get_boundary_by_name(job.boundary_name)
        boundary_dataproc = DataProcBoundary(
            job.boundary_name, boundary_db.geometry, boundary_db.envelope
        )
        # Check processors are all valid and ensure a given boundary-processor-version,
        # combination is not already executing / queued
        internal_processors = currently_active_or_reserved_processors(job.boundary_name)
        for processor_name_version in job.processors:
            module = get_processor_by_name(processor_name_version)
            if not module:
                raise ProcessorNotFoundException(
                    f"Invalid processor.version: {processor_name_version}"
                )
            if processor_name_version in internal_processors:
                raise ProcessorAlreadyExecutingException(
                    f"processor.version {processor_name_version} already executing for boundary {job.boundary_name}"
                )
        dag = create_dag(boundary_dataproc, job.processors)
        # Run DAG
        res = dag.apply_async(task_id=random_task_uuid())
        result = schemas.SubmittedJob(job_id=res.id)
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        return result
    except CannotGetCeleryTasksInfoException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=500,
            detail=f"{str(err)}",
        )
    except ProcessorNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=400,
            detail=f"{str(err)}",
        )
    except ProcessorAlreadyExecutingException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=400,
            detail=f"{str(err)}",
        )
    except BoundaryNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(
            status_code=400,
            detail=f"Requested boundary {job.boundary_name} could not be found",
        )
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
