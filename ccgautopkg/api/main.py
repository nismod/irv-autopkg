"""
FastAPI App Main
"""
from typing import Any, List, Union

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from celery import group, chain
from celery.result import AsyncResult
from pydantic import BaseModel

from dataproc.backends.localfs import LocalFSBackend
from dataproc import tasks
from dataproc.tasks import boundary_setup, generate_provenance
from dataproc.backends.base import Backend
from dataproc.helpers import Boundary

from api.db import database
from api.config import DEPLOYMENT_ENV

app = FastAPI(debug=True if DEPLOYMENT_ENV == 'dev' else False)

@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

class Job(BaseModel):
    boundary_id: int
    boundary_name: str
    boundary_geojson: dict
    processors: List[str]

def get_processor_task(name: str) -> Any:
    """Get task related to a processor task by its name"""
    return getattr(tasks, name)

def create_dag(boundary: Boundary, backend: Backend, processors: List):
    """Generate a DAG for a given call"""

    # Collect the names of dataset processors from user input
    requested_processors = [get_processor_task(name) for name in processors]
    processor_tasks = [proc.s(boundary, backend) for proc in requested_processors]

    # Build the DAG
    step_setup = boundary_setup.s(boundary, backend)
    step_finalise = generate_provenance.s(boundary, backend)
    dag = (step_setup | group(processor_tasks) | step_finalise)
    return dag

@app.get("/")
async def root():
    """Test endpoint"""
    return {"message": "Hello World"}

@app.post("/processors/jobs")
async def submit_processing_job(job: Job):
    """Submit a job"""
    boundary = Boundary(job.boundary_id, job.boundary_name, job.boundary_geojson, 1)
    backend = LocalFSBackend()
    dag = create_dag(boundary, backend, job.processors)
    # Run DAG
    res = dag.apply_async()
    return {"jobid": res.id}

@app.get("/processors/jobs/{task_id}")
def get_status(task_id):
    """Get status of a DAG"""
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JSONResponse(result)
