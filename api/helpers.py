"""
API Helpers
"""

import traceback
from typing import Any, List

from celery import group
from celery.result import GroupResult
from celery.utils import uuid

from api import schemas
from dataproc import tasks, Boundary
from dataproc.tasks import boundary_setup, generate_provenance
from dataproc.helpers import (
    get_processor_meta_by_name,
)
from dataproc.exceptions import InvalidProcessorException
from api.exceptions import (
    CannotGetCeleryTasksInfoException,
)

from config import CELERY_APP, PACKAGES_HOST_URL

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
) -> schemas.ProcessorVersionMetadata:
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
        return schemas.ProcessorVersionMetadata(
            name=processor_name_version,
            description=meta.description,
            version=meta.version,
            status="executing" if executing is True else "complete",
            data_author=meta.data_author,
            data_title=meta.data_title,
            data_title_long=meta.data_title_long,
            data_summary=meta.data_summary,
            data_citation=meta.data_citation,
            data_license=meta.data_license.asdict(),
            data_origin_url=meta.data_origin_url,
            data_formats=meta.data_formats
        )


# Celery Queue Interactions
def extract_group_state_info(group_result: GroupResult) -> schemas.JobGroupStatus:
    """
    Generate job status info from a GroupStatus object

    state=PENDING - info = None
    state=EXECUTING - info = {'progress': int, 'current_task': str} 
        Comes from the processor updating its states
    state=SUCCESS/FAILURE - info = {output of prov log in processor}

    """
    global_perc_complete = []
    global_status = []
    processors = []
    for result in group_result.results:
        global_perc_complete.append(100/len(group_result.results) if result.state in ["SUCCESS", "FAILURE"] else 0)
        global_status.append(True if result.state in ["SUCCESS", "FAILURE"] else False)
        if result.info is not None and isinstance(result.info, dict):
            for proc_name, task_meta in result.info.items():
                if proc_name == "boundary_processor":
                    continue
                if not isinstance(task_meta, dict):
                    continue
                # While its progressing we report the progress, otherwise we show the result
                processors.append(
                    schemas.JobStatus(
                        processor_name=proc_name,
                        job_id=result.id,
                        job_status=result.state,
                        job_progress=schemas.JobProgress(
                            percent_complete=task_meta['progress'] if isinstance(task_meta, dict) and 'progress' in task_meta.keys() else 0,
                            current_task=task_meta['current_task'] if isinstance(task_meta, dict) and 'current_task' in task_meta.keys() else "UNKNOWN",
                        ) if result.state not in ["SUCCESS", "FAILURE"] else None,
                        job_result=task_meta if result.state in ["SUCCESS", "FAILURE"] else None,
                    )
                )
        else:
            # Awaiting execution - attempt to get info direct
            try:
                _result = get_celery_task_info(result.id)
                host = list(_result.keys())[0]
                proc_name = (_result[host][result.id][1]['args'][2])
                processors.append(schemas.JobStatus(
                    processor_name=proc_name,
                    job_id=result.id,
                    job_status=result.state,
                    job_progress=None,
                    job_result=None,
                ))
            except Exception:
                # Sometimes Celery fails to return a result object - when under heavy load
                processors.append(schemas.JobStatus(
                    processor_name="failed to complete task status",
                    job_id=result.id,
                    job_status=result.state,
                    job_progress=None,
                    job_result=None,
                ))
    return schemas.JobGroupStatus(
        job_group_status="COMPLETE" if all(global_status) else "PENDING",
        job_group_percent_complete=sum(global_perc_complete),
        job_group_processors=processors
    )

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
        "celery@host": {
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
                    "hostname": "celery@host",
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
    DEPRECATED

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


def build_package_url(packages_host_url: str, boundary_name: str) -> str:
    """Build the url to top-level package directory for a given boundary"""
    return f"{packages_host_url}/{boundary_name}/"


def build_dataset_version_url(
    packages_host_url: str, boundary_name: str, dataset_name: str, dataset_version: str
) -> str:
    """Build the url a dataset version directory for a given boundary"""
    return f"{packages_host_url}/{boundary_name}/datasets/{dataset_name}/{dataset_version}/"
