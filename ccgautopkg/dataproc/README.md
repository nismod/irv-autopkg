# Data Processors

Celery-based processors for generating file-structures and data for packages.

These processors are built and executed as dynamically generated DAGs in Celery.

## Adding a Processor

- Add the package under dataproc.processors.core ensuring the template structure is matched and the Processor and Meta classes exist

- Add a new task under dataproc.tasks.py (this only needs to match the existing template):

```python
@app.task()
def new_processor_task(sink: Any, boundary: Boundary, backend: Backend):
    """
    Check and if required Generate a dataset for a given boundary

    ::param sink Any Sink for result of previous processor in the group (unused)
    """
    proc = available_processors.new_processor.NewProcessor(boundary, backend)
    proc.generate()
```

- Reboot the API and the processor will be live