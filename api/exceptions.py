"""
Custom Exceptions
"""

class ApiException(Exception):
    pass

class BoundarySearchException(ApiException):
    """Error handling a requested search"""

class BoundaryNotFoundException(ApiException):
    """"""

class ProcessorNotFoundException(ApiException):
    """Processor with a given name could not be verified"""

class ProcessorAlreadyExecutingException(ApiException):
    """Processor with a given name and version is already executing against a given boundary"""

class JobNotFoundException(ApiException):
    """Job with a given id could not be found"""

class PackageHasNoDatasetsException(ApiException):
    """The given package had no executing or existing datasets"""

class CannotGetExecutingTasksException(ApiException):
    """Call to Celery for processing tasks failed"""

class CannotGetCeleryTasksInfoException(ApiException):
    """Call to Celery for ACtive / Reserved tasks failed"""
