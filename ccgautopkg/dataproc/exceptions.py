"""
Custom Dataproc exceptions
"""

class DataProcException(Exception):
    pass

class ConfigException(DataProcException):
    """Error with configuration """

class InvalidProcessorException(DataProcException):
    """A processor cannot be used / is invalid"""

class FolderCreationException(DataProcException):
    """Error creating a folder"""

class FileCreationException(DataProcException):
    """Error creating a file"""

class PackageNotFoundException(DataProcException):
    """Package with a given name does not exist on the StorageBackend"""

class DatasetNotFoundException(DataProcException):
    """Dataset with within a package does not exist on the StorageBackend"""

