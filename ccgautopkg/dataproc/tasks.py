"""
Processor Task Wrappers
"""
import os
from typing import Any

from celery import Celery

from config import (
    CELERY_BACKEND,
    CELERY_BROKER,
    STORAGE_BACKEND,
    LOCALFS_STORAGE_BACKEND_ROOT,
    PROCESSING_BACKEND,
    LOCALFS_PROCESSING_BACKEND_ROOT,
)
from dataproc import Boundary
from dataproc.helpers import (init_storage_backend, init_processing_backend, get_processor_by_name)
from dataproc.processors.internal import (
    BoundaryProcessor,
    ProvenanceProcessor,
)


app = Celery("CCG-AutoPackage")
# app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
# app.conf.result_backend = os.environ.get(
#     "CELERY_RESULT_BACKEND", "redis://localhost:6379"
# )
app.conf.broker_url = CELERY_BROKER
app.conf.result_backend = CELERY_BACKEND


# Setup Configured Processing Backend
processing_backend = init_processing_backend(PROCESSING_BACKEND)(LOCALFS_PROCESSING_BACKEND_ROOT)
# Setup Configured Storage Backend
storage_backend = init_storage_backend(STORAGE_BACKEND)(LOCALFS_STORAGE_BACKEND_ROOT)

# SETUP TASK
@app.task()
def boundary_setup(boundary: Boundary) -> bool:
    """Instantiate the top-level structure for a boundary"""
    proc = BoundaryProcessor(boundary, storage_backend, processing_backend)
    proc.generate()
    return "boundary setup done"


# DATASET PROCESSOR TASK
@app.task()
def processor_task(sink: Any, boundary: Boundary, processor_name_version: str):
    """
    Generic task that implements a processor

    ::param sink Any Sink for result of previous processor in the group
    """
    module = get_processor_by_name(processor_name_version)
    proc = module(
        boundary, storage_backend, processing_backend
    )
    proc.generate()
    # Potentially do this during execution - get the progress from the processor
    # self.update_state(state="PROGRESS", meta={'progress': 50})
    # See: https://docs.celeryq.dev/en/stable/userguide/calling.html#on-message



# COMPLETION TASK
@app.task()
def generate_provenance(sink: Any, boundary: Boundary):
    """Generate / update the processing provenance for a given boundary"""
    proc = ProvenanceProcessor(boundary, storage_backend, processing_backend)
    res = proc.generate()
    return res


# HELPER TASKS
@app.task
def processor_group_finisher():
    """
    Serves as a sink-task which follows a group of tasks run in a Chord
        __NOTE__: this is required to enable a DAG
            to include a group of processor tasks to be run in parallel,
            where there is no inter-dependence between the given Group tasks
    """
    return "Processor group results: OK"
