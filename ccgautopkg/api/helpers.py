"""
API Helpers
"""

import traceback
import inspect
from typing import Any, List
from types import ModuleType

from celery import group

from dataproc.processors.internal.base import BaseProcessorABC
from dataproc.helpers import Boundary
from dataproc import tasks
from dataproc.tasks import boundary_setup, generate_provenance
from dataproc.backends import StorageBackend
from dataproc.backends.storage.localfs import LocalFSStorageBackend

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

def init_storage_backend(storage_backend: str) -> StorageBackend:
    """
    Initialise a StorageBackend by name
    """
    if storage_backend == 'localfs':
        return LocalFSStorageBackend

def processor_name(dataset: str, version: str) -> str:
    """Generate a processor name from a dataset and version"""
    return f"{dataset}.{version}"

def valid_processor(name: str, processor: BaseProcessorABC) -> bool:
    """Check if a Processor is valid and can be used"""
    if name in ['_module', 'pkgutil']:
        return False
    # Skip top level modules without metadata
    if not hasattr(processor, "Metadata"):
        return False
    if isinstance(processor, ModuleType):
        # Ensure its versioned
        if "version" in name:
            return True
    return False

def build_processor_name_version(processor_base_name: str, version: str) -> str:
    """Build a full processor name from name and version"""
    return f"{processor_base_name}.{version}"

def list_processors() -> List[BaseProcessorABC]:
    """Retrieve a list of available processors and their versions"""
    # Iterate through Core processors and collect metadata
    import dataproc.processors.core as available_processors
    valid_processors = {} # {name: [versions]}
    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        # Split name and version
        proc_name, proc_version = name.split(".")
        if proc_name in valid_processors.keys():
            valid_processors[proc_name].append(proc_version)
        else:
            valid_processors[proc_name] = [proc_version]
    return valid_processors

def get_processor_by_name(processor_name_version: str) -> BaseProcessorABC:
    """Retrieve a processor module by its name (including version)"""
    import dataproc.processors.core as available_processors
    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        if name == processor_name_version:
            print ("Available:", name)
            return processor

def get_processor_task(name: str) -> Any:
    """Get task related to a processor task by its name"""
    return getattr(tasks, name)


def create_dag(boundary: Boundary, storage_backend: StorageBackend, processors: List):
    """Generate a DAG for a given call"""

    # Collect the names of dataset processors from user input
    requested_processors = [get_processor_task(name) for name in processors]
    processor_tasks = [proc.s(boundary, storage_backend) for proc in requested_processors] # .s generates a signature through instantiation

    # Build the DAG
    step_setup = boundary_setup.s(boundary, storage_backend)
    step_finalise = generate_provenance.s(boundary, storage_backend)
    dag = step_setup | group(processor_tasks) | step_finalise
    return dag

