"""
Processor Task Wrappers
"""
from typing import Any, List
from contextlib import contextmanager
import logging

from celery import signals
from celery.utils.log import get_task_logger
from redis import Redis

from config import (
    LOG_LEVEL,
    CELERY_APP,
    TASK_LOCK_TIMEOUT,
    STORAGE_BACKEND,
    LOCALFS_STORAGE_BACKEND_ROOT,
    LOCALFS_PROCESSING_BACKEND_ROOT,
    REDIS_HOST,
)
from dataproc import Boundary
from dataproc.helpers import get_processor_by_name, get_processor_meta_by_name
from dataproc.processors.internal import (
    BoundaryProcessor,
    ProvenanceProcessor,
)
from dataproc.exceptions import ProcessorAlreadyExecutingException
from dataproc.backends.storage import init_storage_backend

# Setup Configured Storage Backend
storage_backend = init_storage_backend(STORAGE_BACKEND)(LOCALFS_STORAGE_BACKEND_ROOT)

# Used for guarding against parallel execution of duplicate tasks
redis_client = Redis(host=REDIS_HOST)


@contextmanager
def redis_lock(task_sig: str):
    """
    Manage Task execution lock within redis
    """
    if redis_client.exists(task_sig):
        raise ProcessorAlreadyExecutingException()
    yield redis_client.setex(task_sig, TASK_LOCK_TIMEOUT, value="")


def task_signature(boundary_name: str, processor: str):
    """
    Generate a signature of a task
        based on boundary name and processor name.version
    """
    return f"{boundary_name}.{processor}"


@signals.after_setup_task_logger.connect
def quieter_fiona_logging(logger, *args, **kwargs):
    """
    Fiona package is really verbose at DEBUG -
        e.g. it logs every line of a processed CSV,
        so we turn it down here
    """
    logging.getLogger("fiona").propagate = False

@signals.after_setup_logger.connect
def config_logging(logger, *args, **kwargs):
    """"""
    logger.setLevel(LOG_LEVEL)


# SETUP TASK
@CELERY_APP.task()
def boundary_setup(boundary: Boundary) -> dict:
    """
    Instantiate the top-level structure for a boundary

    Includes task lock context manager to ensure duplicate tasks are not executed in parallel.
    Duplicate tasks are skipped in this instance.

    """
    logger = get_task_logger(__name__)
    task_sig = task_signature(boundary["name"], "boundary_setup")
    try:
        with redis_lock(task_sig) as acquired:
            if acquired:
                try:
                    proc = BoundaryProcessor(boundary, storage_backend)
                    result = proc.generate()
                    return result
                except Exception as err:
                    logger.exception("")
                    # Update sink for this processor
                    return {"boundary_processor": {"failed": type(err).__name__}}
                finally:
                    _ = redis_client.getdel(task_sig)
            else:
                raise ProcessorAlreadyExecutingException()
    except ProcessorAlreadyExecutingException:
        logger.warning(
            "task with signature %s skipped because it was already executing", task_sig
        )
        return {"boundary_processor": {"skipped": f"{task_sig} already executing"}}


# DATASET PROCESSOR TASK
@CELERY_APP.task(bind=True)
def processor_task(
    self, sink: dict, boundary: Boundary, processor_name_version: str
) -> dict:
    """
    Generic task that implements a processor

    Includes task lock context manager to ensure duplicate tasks are not executed in parallel.
    Duplicate tasks are skipped in this instance.

    ::param sink Any Sink for result of previous processor in the group
    """
    logger = get_task_logger(__name__)
    task_sig = task_signature(boundary["name"], processor_name_version)
    try:
        with redis_lock(task_sig) as acquired:
            if acquired:
                try:
                    module = get_processor_by_name(processor_name_version)
                    module_meta = get_processor_meta_by_name(processor_name_version)
                    with module(
                        module_meta,
                        boundary,
                        storage_backend,
                        self,
                        LOCALFS_PROCESSING_BACKEND_ROOT,
                    ) as proc:
                        result = proc.generate()
                    # Update sink for this processor
                    sink[processor_name_version] = result
                except Exception as err:
                    logger.exception("")
                    # Update sink for this processor
                    sink[processor_name_version] = {"failed": type(err).__name__}
                finally:
                    _ = redis_client.getdel(task_sig)
                return sink
            else:
                raise ProcessorAlreadyExecutingException()
    except ProcessorAlreadyExecutingException:
        logger.warning(
            "task with signature %s skipped because it was already executing", task_sig
        )
        sink[processor_name_version] = {"skipped": f"{task_sig} already executing"}
        return sink


# COMPLETION TASK
@CELERY_APP.task(bind=True)
def generate_provenance(self, sink: Any, boundary: Boundary):
    """
    Generate / update the processing provenance for a given boundary
    Includes task lock context manager to ensure duplicate tasks are not executed in parallel.
    Duplicate tasks are retried in this instance so we always generate provenance.

    """
    logger = get_task_logger(__name__)
    task_sig = task_signature(boundary["name"], "generate_provenance")
    try:
        with redis_lock(task_sig) as acquired:
            if acquired:
                try:
                    # The sink can come in as a list (multiple processors ran) or dict (one processor ran)
                    if isinstance(sink, dict):
                        sink = [sink]
                    proc = ProvenanceProcessor(boundary, storage_backend)
                    res = proc.generate(sink)
                except Exception as err:
                    logger.exception("")
                    # Update sink for this processor
                    sink["generate_provenance"] = {"failed": type(err).__name__}
                finally:
                    _ = redis_client.getdel(task_sig)
            else:
                raise ProcessorAlreadyExecutingException()
    except ProcessorAlreadyExecutingException:
        logger.warning(
            "task with signature %s skipped because it was already executing", task_sig
        )
        self.retry(countdown=5)
    return res
