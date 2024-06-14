"""
Processing Job Endpoints
"""

import logging

from fastapi import APIRouter, HTTPException, status

from config import LOG_LEVEL
from api import schemas
from api.routes import JOB_STATUS_ROUTE, JOBS_BASE_ROUTE

router = APIRouter(
    tags=["jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)
logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)


@router.get(
    JOB_STATUS_ROUTE,
    response_model=schemas.JobGroupStatus,
    response_model_exclude_none=True,
)
def get_status(job_id: str):
    """Get status of a DAG associated with a given package"""
    raise HTTPException(status_code=404, detail=f"Job not found: {str(job_id)}")


@router.post(JOBS_BASE_ROUTE, response_model=schemas.SubmittedJob)
async def submit_processing_job(job: schemas.Job, status_code=status.HTTP_202_ACCEPTED):
    """Submit a job for a given package to run a list of dataset-processors"""
    raise HTTPException(
        status_code=400,
        detail="No jobs are currently being accepted.",
    )
