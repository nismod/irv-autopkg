"""
Processor Task Wrappers
"""
from typing import Any

from celery import Celery

from config import (
    CELERY_APP,
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


# Setup Configured Processing Backend
processing_backend = init_processing_backend(PROCESSING_BACKEND)(LOCALFS_PROCESSING_BACKEND_ROOT)
# Setup Configured Storage Backend
storage_backend = init_storage_backend(STORAGE_BACKEND)(LOCALFS_STORAGE_BACKEND_ROOT)

# SETUP TASK
@CELERY_APP.task()
def boundary_setup(boundary: Boundary) -> bool:
    """Instantiate the top-level structure for a boundary"""
    proc = BoundaryProcessor(boundary, storage_backend, processing_backend)
    proc.generate()
    return "boundary setup done"


# DATASET PROCESSOR TASK
@CELERY_APP.task()
def processor_task(sink: Any, boundary: Boundary, processor_name_version: str):
    """
    Generic task that implements a processor

    ::param sink Any Sink for result of previous processor in the group
    """
    module = get_processor_by_name(processor_name_version)
    proc = module(
        boundary, storage_backend, processing_backend
    )
    res = proc.generate()
    return str(sink) + "|" + str(res) + "-" + processor_name_version
    # Potentially do this during execution - get the progress from the processor
    # self.update_state(state="PROGRESS", meta={'progress': 50})
    # See: https://docs.celeryq.dev/en/stable/userguide/calling.html#on-message



# COMPLETION TASK
@CELERY_APP.task()
def generate_provenance(sink: Any, boundary: Boundary):
    """Generate / update the processing provenance for a given boundary"""
    proc = ProvenanceProcessor(boundary, storage_backend, processing_backend)
    res = proc.generate(sink)
    return res
