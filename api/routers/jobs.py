"""
Processing Job Endpoints
"""
import logging
import inspect

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from celery.result import GroupResult, AsyncResult, ResultBase

from config import LOG_LEVEL, CELERY_APP, TASK_EXPIRY_SECS
from dataproc import Boundary as DataProcBoundary
from dataproc.helpers import get_processor_by_name
from api import schemas
from api.helpers import (
    handle_exception,
    create_dag,
    random_task_uuid,
    extract_group_state_info,
)
from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE
from api.exceptions import (
    BoundaryNotFoundException,
    ProcessorNotFoundException,
    JobNotFoundException,
    ProcessorAlreadyExecutingException,
    CannotGetCeleryTasksInfoException,
)
from api.db import DBController

router = APIRouter(
    tags=["jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)


def extract_job_id(node):
    """
    Iterate through the DAG to retrieve the GroupResult and make sure its saved

    Return the second result ID - which is always the processor task,
        whether group result or async result
    """
    proc_id = None
    dag_node = node
    while node.parent:
        if isinstance(node, GroupResult):
            node.save()
            proc_id = node.id
        node = node.parent
    if not proc_id:
        # Processors always the second in DAG
        proc_id = dag_node.parent.id
    return proc_id


@router.get(
    JOB_STATUS_ROUTE,
    response_model=schemas.JobGroupStatus,
    response_model_exclude_none=True,
)
def get_status(job_id: str):
    """Get status of a DAG associated with a given package"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        # Single processor tasks are AsyncResult instead of GroupResult
        # Collect the current status of the Group result within the DAG (the processor tasks)
        group_result = CELERY_APP.GroupResult.restore(
            job_id, backend=CELERY_APP.backend
        )
        if not group_result:
            # Attempt to get it as a single result
            result = AsyncResult(job_id, CELERY_APP.backend)
            if not result:
                raise JobNotFoundException(f"{job_id}")
            group_result = GroupResult(job_id, [result], ResultBase())
        # Remove Boundary processor from each jobs info
        logger.debug(
            "Group Status: %s",
            [
                [result.state, result.info, result.args, result.name]
                for result in group_result.results
            ],
        )
        # Create response object
        response = extract_group_state_info(
            group_result, missing_proc_name_msg=schemas.MISSING_PROC_MSG
        )
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], response)
        return response
    except JobNotFoundException as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=404, detail=f"Job not found: {str(err)}")
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.post(JOBS_BASE_ROUTE, response_model=schemas.SubmittedJob)
async def submit_processing_job(job: schemas.Job, status_code=status.HTTP_202_ACCEPTED):
    """Submit a job for a given package to run a list of dataset-processors"""
    try:
        logger.debug("performing %s", inspect.stack()[0][3])
        # Collect boundary geojson
        boundary_db = await DBController().get_boundary_by_name(job.boundary_name)
        boundary_dataproc = DataProcBoundary(
            job.boundary_name,
            boundary_db.geometry.dict()["__root__"],
            boundary_db.envelope.dict()[
                "__root__"
            ],  # Due to GeoJSON dynamic type in external schema
        )
        # Check processors are all valid
        for processor_name_version in job.processors:
            module = get_processor_by_name(processor_name_version)
            if not module:
                raise ProcessorNotFoundException(
                    f"Invalid processor.version: {processor_name_version}"
                )
        dag = create_dag(boundary_dataproc, job.processors)
        # Run DAG
        dag_node = dag.apply_async(
            task_id=random_task_uuid(), retry=False, expires=TASK_EXPIRY_SECS
        )
        # Extract the GroupResult task ID if multiple Processors have been requested,
        # otherwise its an AsyncResult
        processor_group_id = extract_job_id(dag_node)
        result = schemas.SubmittedJob(job_id=processor_group_id)
        logger.debug("completed %s with result: %s", inspect.stack()[0][3], result)
        encoded_result = jsonable_encoder(result)
        return JSONResponse(status_code=status_code, content=encoded_result)
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
