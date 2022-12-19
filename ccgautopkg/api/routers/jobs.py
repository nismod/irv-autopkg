"""
Processing Job Endpoints
"""
import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from celery.result import AsyncResult

from dataproc.helpers import Boundary
from dataproc.backends.localfs import LocalFSBackend

from api.helpers import create_dag, handle_exception
from api.schemas import Job
from api.config import LOG_LEVEL
from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE

router = APIRouter(
    tags=["jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(logging.DEBUG)

@router.post(JOBS_BASE_ROUTE)
async def submit_processing_job(package_id: str, job: Job):
    """Submit a job for a given package"""
    try:
        boundary = Boundary(job.boundary_id, job.boundary_name, job.boundary_geojson, 1)
        backend = LocalFSBackend()
        dag = create_dag(boundary, backend, job.processors)
        # Run DAG
        res = dag.apply_async()
        return {"jobid": res.id}
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)

@router.get(JOB_STATUS_ROUTE)
def get_status(package_id: str, task_id: str):
    """Get status of a DAG associated with a given package"""
    try:
        task_result = AsyncResult(task_id)
        result = {
            "task_id": task_id,
            "task_status": task_result.status,
            "task_result": task_result.result
        }
        return JSONResponse(result)
    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)