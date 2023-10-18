"""Custom Dataproc exceptions
"""


class DataProcException(Exception):
    pass


class ConfigException(DataProcException):
    """Error with configuration"""


class ProcessorExecutionFailed(DataProcException):
    """A Processor failed during execution"""


class ProcessorExecutionSkipped(DataProcException):
    """A Processor was skipped during execution"""


class ProcessorDatasetExists(DataProcException):
    """Processor output for a given boundary already exists"""


class ProcessorAlreadyExecutingException(DataProcException):
    """A Given processor, boundary combination is already executing"""


class InvalidProcessorException(DataProcException):
    """A processor cannot be used / is invalid"""


class FolderCreationException(DataProcException):
    """Error creating a folder"""


class FolderNotFoundException(DataProcException):
    """A Foler could not be found on the backend"""


class FileCreationException(DataProcException):
    """Error creating a file"""


class PackageNotFoundException(DataProcException):
    """Package with a given name does not exist on the StorageBackend"""


class DatasetNotFoundException(DataProcException):
    """Dataset with within a package does not exist on the StorageBackend"""


class SourceRasterProjectionException(DataProcException):
    """Prohblem with source raster projection"""


class UnexpectedFilesException(DataProcException):
    """Unexpected files encountered during execution"""


class ZenodoGetFailedException(DataProcException):
    """Zenodo Get command returned non-zero result"""


class S3Exception(DataProcException):
    """Wrapper for exceptions generated by S3 filesystem"""
