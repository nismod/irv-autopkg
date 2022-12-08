"""
Processor Task Wrappers
"""
import os
from typing import Any

from celery import Celery

from dataproc.backends.base import Backend
from dataproc.helpers import Boundary
from dataproc.processors import BoundaryProcessor, ProvenanceProcessor, RasterProcessorOne, RasterProcessorTwo

app = Celery('proj')
app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

# SETUP TASK
@app.task()
def boundary_setup(boundary: Boundary, backend: Backend) -> bool:
    """Instantiate the top-level structure for a boundary"""
    proc = BoundaryProcessor(boundary, backend)
    proc.generate()
    return "boundary setup done"


# DATASET PROCESSOR TASKS
@app.task()
def raster_processor_one(sink: Any, boundary: Boundary, backend: Backend):
    """
    Check and if required Generate a dataset for a given boundary

    ::param sink Any Sink for result of previous processor in the group (unused)
    """
    proc = RasterProcessorOne(boundary, backend)
    proc.generate()
    # Potentially do this during execution - get the progress from the processor
    # self.update_state(state="PROGRESS", meta={'progress': 50})
    # See: https://docs.celeryq.dev/en/stable/userguide/calling.html#on-message

@app.task()
def raster_processor_two(sink: Any, boundary: Boundary, backend: Backend):
    """
    Check and if required Generate a dataset for a given boundary

    ::param sink Any Sink for result of previous processor in the group (unused)
    """
    proc = RasterProcessorTwo(boundary, backend)
    proc.generate()


# COMPLETION TASK
@app.task()
def generate_provenance(sink: Any, boundary: Boundary, backend: Backend):
    """Generate / update the processing provenance for a given boundary"""
    proc = ProvenanceProcessor(boundary, backend)
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
