"""
API Helpers
"""

import traceback
from typing import Any, List

from celery import group

from dataproc.helpers import Boundary
from dataproc import tasks
from dataproc.tasks import boundary_setup, generate_provenance
from dataproc.backends.base import Backend

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

# DAGs and Processing


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
    dag = step_setup | group(processor_tasks) | step_finalise
    return dag


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
