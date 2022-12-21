"""
API Helpers
"""

import traceback
from typing import Any, List

from celery import group

from dataproc import tasks, Boundary
from dataproc.tasks import boundary_setup, generate_provenance

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