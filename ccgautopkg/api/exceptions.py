"""
Custom Exceptions
"""

class ApiException(Exception):
    pass

class BoundarySearchException(ApiException):
    """Error handling a requested search"""
    pass

class BoundaryNotFoundException(ApiException):
    """"""
    pass

class ProcessorNotFoundException(ApiException):
    """Processor with a given name could not be verified"""
    pass

class JobNotFoundException(ApiException):
    """Job with a given id could not be found"""
    pass


