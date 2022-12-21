"""
Helper methods / classes
"""
import inspect
from typing import List
from types import ModuleType

from dataproc.processors.internal.base import BaseProcessorABC
from dataproc.backends import StorageBackend, ProcessingBackend
from dataproc.backends.storage.localfs import LocalFSStorageBackend
from dataproc.backends.processing.localfs import LocalFSProcessingBackend


# DAGs and Processing

def init_storage_backend(storage_backend: str) -> StorageBackend:
    """
    Initialise a StorageBackend by name
    """
    if storage_backend == 'localfs':
        return LocalFSStorageBackend

def init_processing_backend(processing_backend: str) -> ProcessingBackend:
    """
    Initialise a ProcessingBackend by name
    """
    if processing_backend == 'localfs':
        return LocalFSProcessingBackend

def processor_name(dataset: str, version: str) -> str:
    """Generate a processor name from a dataset and version"""
    return f"{dataset}.{version}"

def dataset_name_from_processor(processor_name_version: str) -> str:
    """Generate a dataset name from a processor name ane version"""
    return processor_name_version.split(".")[0]


def valid_processor(name: str, processor: BaseProcessorABC) -> bool:
    """Check if a Processor is valid and can be used"""
    if name in ["_module", "pkgutil"]:
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

    valid_processors = {}  # {name: [versions]}
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
            return processor.Processor

def get_processor_meta_by_name(processor_name_version: str) -> BaseProcessorABC:
    """Retrieve a processor MetaData module by its name (including version)"""
    import dataproc.processors.core as available_processors

    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        if name == processor_name_version:
            return processor.Metadata