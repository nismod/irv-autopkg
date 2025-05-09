"""
Custom Exceptions
"""


class ApiException(Exception):
    pass


class BoundarySearchException(ApiException):
    """Error handling a requested search"""


class BoundaryNotFoundException(ApiException):
    """"""
