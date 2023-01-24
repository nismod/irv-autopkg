"""
API Helpers
"""

import traceback
import json
import base64
from typing import Any, List
import urllib.parse

from celery import group
from celery.utils import uuid
from api import schemas

from dataproc import tasks, Boundary
from dataproc.tasks import boundary_setup, generate_provenance
from dataproc.helpers import (
    processor_name,
    get_processor_meta_by_name,
    build_processor_name_version,
)
from dataproc.exceptions import InvalidProcessorException
from api.exceptions import (
    CannotGetExecutingTasksException,
    CannotGetCeleryTasksInfoException,
)

from config import CELERY_APP, PACKAGES_HOST

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

    # Build the DAG
    step_setup = boundary_setup.s(boundary)
    step_finalise = generate_provenance.s(boundary)
    dag = step_setup | group(processor_task_signatures) | step_finalise
    return dag


def random_task_uuid():
    """Generate a custom task uuid"""
    return uuid()


def processor_meta(
    processor_name_version: str, executing: bool = False
) -> schemas.ProcessorVersion:
    """
    Generate ProcessorVersion (with nested metadata) for a given processor version
    """
    meta_cls = get_processor_meta_by_name(processor_name_version)
    if not meta_cls:
        # Either the processor is missing its meta or
        # the dataset name on the FS has no applicable processor
        raise InvalidProcessorException()
    if meta_cls is not None:
        meta = meta_cls()
        return schemas.ProcessorVersion(
            processor=schemas.ProcessorMetadata(
                name=processor_name_version,
                description=meta.description,
                dataset=meta.dataset_name,
                author=meta.data_author,
                license=meta.data_license.asdict(),
                origin_url=meta.data_origin_url,
                version=meta.version,
                status="executing" if executing is True else "complete",
            ),
            version=meta.version
        )


# Celery Queue Interactions


def get_celery_active_tasks() -> dict:
    """Return list of tasks currently executed by Celery workers"""
    task_inspector = CELERY_APP.control.inspect()
    return task_inspector.active()


def get_celery_registered_tasks() -> dict:
    """List of Actual task types that have been registered to Workers (not actual jobs)"""
    task_inspector = CELERY_APP.control.inspect()
    return task_inspector.registered()


def get_celery_reserved_tasks() -> dict:
    """Return list of tasks currently queued by Celery workers"""
    task_inspector = CELERY_APP.control.inspect()
    return task_inspector.reserved()


def get_celery_scheduled_tasks() -> dict:
    """Return list of tasks currently scheduled by Celery workers"""
    task_inspector = CELERY_APP.control.inspect()
    return task_inspector.scheduled()


def get_celery_task_info(task_id: str) -> dict:
    """
        Information about a specific task
         Example Output
         {
        "celery@dusteds-MBP": {
            "bcc2178e-4c1a-42fc-9615-783cfd602a14": [
                "active",
                {
                    "id": "bcc2178e-4c1a-42fc-9615-783cfd602a14",
                    "name": "dataproc.tasks.processor_task",
                    "args": [
                        {
                            "boundary_processor": {
                                "boundary_folder": "exists"
                            }
                        },
                        {
                            "name": "gambia",
                            "geojson": {
                                "type": "MultiPolygon",
                                "coordinates": []
                            },
                            "envelope_geojson": {
                                "type": "Polygon",
                                "coordinates": []
                            }
                        },
                        "test_processor.version_1"
                    ],
                    "kwargs": {},
                    "type": "dataproc.tasks.processor_task",
                    "hostname": "celery@dusteds-MBP",
                    "time_start": 1673439269.0976949,
                    "acknowledged": True,
                    "delivery_info": {
                        "exchange": "",
                        "routing_key": "celery",
                        "priority": 0,
                        "redelivered": None
                    },
                    "worker_pid": 58536
                }
            ]
        }
    }
    """
    task_inspector = CELERY_APP.control.inspect()
    return task_inspector.query_task(task_id)


def _task_arg_contains_boundary(boundary_name: str, task: dict) -> bool:
    """
    Whether a given task info shows it is instantiated against a given boundary
    """
    if task["type"] == "dataproc.tasks.processor_task":
        # Match task to requested boundary name
        if (
            task["args"][1]["name"] == boundary_name
        ):  # see tasks.py -> processor_task args
            return True
    return False


def currently_active_or_reserved_processors(boundary_name: str) -> List[str]:
    """
    Collect a list of name.version entries for
        processors wither executing (active) or queued (reserved)
        against a given boundary name
    """
    active_tasks = get_celery_active_tasks()
    reserved_tasks = get_celery_reserved_tasks()
    if active_tasks is None or reserved_tasks is None:
        # The processing backend has probably failed / is not running
        raise CannotGetCeleryTasksInfoException()
    processors = []
    for worker, worker_executing_tasks in active_tasks.items():
        for task in worker_executing_tasks:
            if _task_arg_contains_boundary(boundary_name, task):
                processors.append(task["args"][2])
    for worker, worker_executing_tasks in reserved_tasks.items():
        for task in worker_executing_tasks:
            if _task_arg_contains_boundary(boundary_name, task):
                processors.append(task["args"][2])
    return processors


def build_package_url(packages_host: str, boundary_name: str) -> str:
    """Build the url to top-level package directory for a given boundary"""
    return urllib.parse.urljoin(packages_host, f"packages/{boundary_name}/")


def build_dataset_version_url(
    packages_host: str, boundary_name: str, dataset_name: str, dataset_version: str
) -> str:
    """Build the url a dataset version directory for a given boundary"""
    return urllib.parse.urljoin(
        packages_host,
        f"packages/{boundary_name}/datasets/{dataset_name}/{dataset_version}/",
    )
