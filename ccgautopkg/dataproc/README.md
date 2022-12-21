# Data Processors

Celery-based processors for generating file-structures and data for packages.

These processors are built and executed as dynamically generated DAGs in Celery.

## Adding a Processor

- Add the package version under dataproc.processors.core ensuring the template structure is matched and the Processor and Meta classes exist for the version:

```python
class Metadata(BaseMetadataABC):
    """Processor metadata"""
    name="test_processor" # this must follow snakecase formatting, without special chars
    description="A test processor for nightlights" # Logner processor description
    version="1" # Version of the Processor
    dataset_name="nightlights" # The dataset this processor targets
    data_author="Nightlights Author"
    data_license="Nightlights License"
    data_origin_url="http://url"

class Processor(BaseProcessorABC):
    def __init__(self, boundary: Boundary, storage_backend: StorageBackend, processing_backend: ProcessingBackend) -> None:
        """
        NOTE: Init vars arrive as Dictionaries because the Base-class has to inherit from dict for Celery to serialise

        ::param boundary dict Definition of the boundary
        ::param storage_backend dict Storage backend
        ::param processing_backend dict A backend used for processing (tmp file storage etc)
        """
        self.boundary = boundary
        self.storage_backend = storage_backend
        self.processing_backend = processing_backend

    def generate(self) -> Any:
        """Generate files for a given processor"""
        return True

    def exists(self) -> Any:
        """Whether all files for a given processor exist on the FS on not"""
        pass
```

- Reboot API and Celery Worker to make processor live