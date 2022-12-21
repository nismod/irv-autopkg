"""
API Helpers
"""

import traceback
import json
import base64
from typing import Any, List

from celery import group
from api import schemas

from dataproc import tasks, Boundary
from dataproc.tasks import boundary_setup, generate_provenance, app
from dataproc.helpers import processor_name, get_processor_meta_by_name, build_processor_name_version

# API

OPENAPI_TAGS_META = [
    {
        "name": "boundaries",
        "description": "Detail about boundaries available for generating packages against",
    },
    {
        "name": "packages",
        "description": "Detail about existing packages",
    },
    {
        "name": "jobs",
        "description": "Details and management of processing jobs for packages",
    },
    {
        "name": "processors",
        "description": "Details about processors and associated datasets",
    },
]

# Logging & Error Handling


def handle_exception(logger, err: Exception):
    """
    Handle generic exceptions
    """
    logger.error(
        "%s failed with tb %s, error: %s",
        __name__,
        traceback.format_tb(err.__traceback__),
        err,
    )


# DAGs and Processing


def get_processor_task(name: str) -> Any:
    """Get task related to a processor task by its name"""
    return getattr(tasks, name)


def create_dag(boundary: Boundary, processors: List[str]):
    """
    Generate a DAG of processing jobs for a given boundary

    ::param proocessors List[str] List of processor name with its version:
        <processor>.version_<number>
    """

    # Generate blank tasks for each requested processor
    processor_tasks = [getattr(tasks, "processor_task") for _ in processors]
    # Map these tasks into each actual processor (happens internally in tasks.processor_task using processor name)
    processor_task_signatures = []
    for idx, processor_task in enumerate(processor_tasks):
        processor_task_signatures.append(processor_task.s(boundary, processors[idx]))

    # processor_tasks = [
    #     proc.s(boundary) for proc in processors
    # ]  # .s generates a signature through instantiation

    # Build the DAG
    step_setup = boundary_setup.s(boundary)
    step_finalise = generate_provenance.s(boundary)
    dag = step_setup | group(processor_task_signatures) | step_finalise
    return dag

def processor_meta(processor_name_version: str, executing:bool=False) -> schemas.ProcessorVersion:
    """
    Generate ProcessorVersion (with nested metadata) for a given procesor version
    """
    meta = get_processor_meta_by_name(processor_name_version)()
    if meta is not None:
        return schemas.ProcessorVersion(
                processor=schemas.ProcessorMetadata(
                    name=processor_name_version,
                    description=meta.description,
                    dataset=meta.dataset_name,
                    author=meta.data_author,
                    license=meta.data_license,
                    origin_url=meta.data_origin_url,
                    version=meta.version,
                    status="executing" if executing is True else "complete"
                ),
                version=meta.version,
            )

# Celery Queue Interactions

def get_celery_executing_tasks() -> dict:
    """Return list of tasks currently executed by Celery workers"""
    task_inspector = app.control.inspect()
    return task_inspector.active()


def currently_executing_processors(boundary_name: str) -> List[str]:
    """
    Collect a list of currently executing processors for a given package
    
    Example Celery inspect output:
        {
        "celery@dusteds-MBP": [
            {
                "id": "06d2528a-1a64-459c-b6cf-bf019b66a19d",
                "name": "dataproc.tasks.processor_task",
                "args": [
                    "boundary setup done",
                    {
                        "name": "ghana",
                        "geojson": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        ...
                                    ]
                                ]
                            ],
                        },
                    },
                    "test_processor.version_1",
                ],
                "kwargs": {},
                "type": "dataproc.tasks.processor_task",
                "hostname": "celery@worker",
                "time_start": 1671623963.9834435,
                "acknowledged": True,
                "delivery_info": {
                    "exchange": "",
                    "routing_key": "celery",
                    "priority": 0,
                    "redelivered": None,
                },
                "worker_pid": 67036,
            }
        ]
    }
    """
    # Filter the currently executing tasks for the given boundary
    executing_tasks = get_celery_executing_tasks()
    executing_processors = []
    for worker, worker_executing_tasks in executing_tasks.items():
        for task in worker_executing_tasks:
            if task['type'] == "dataproc.tasks.processor_task":
                # Match task to requested boundary name
                if task['args'][1]['name'] == boundary_name: # see tasks.py -> processor_task args
                    executing_processors.append(task['args'][2])
    return executing_processors
