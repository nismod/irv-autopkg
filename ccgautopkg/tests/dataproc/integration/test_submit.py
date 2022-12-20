"""
Test submit tasks
"""

import sys
from typing import Any

from celery import group, chain, chord
from celery.result import AsyncResult
from dataproc.helpers import Boundary
from dataproc.backends import LocalFSBackend
from dataproc import tasks
from dataproc.tasks import boundary_setup, generate_provenance
from dataproc.tasks import app

def get_processor_task(name: str) -> Any:
    """Get task related to a processor task by its name"""
    return getattr(tasks, name)


if __name__ == "__main__":
    # Basic DAG
    # step_setup = create_bucket.s("test_one")
    # step_processors = chord(
    #     group(
    #         [add.s(1, 1),
    #         mul.s(1, 1)]
    #     ),
    #     processor_finisher.si()
    # )
    # step_finalise = write_metadata.s("test_one")
    # dag = chain(step_setup, step_processors, step_finalise)()
    # res = dag.get()
    # print (res)

    print(sys.argv[1])

    # Setup Job Meta
    boundary = Boundary(1, "uk", {}, "1")
    backend = LocalFSBackend()

    # Collect the names of dataset processors from user input
    proc_names = sys.argv[1].split(',')
    requested_processors = [get_processor_task(name) for name in proc_names]
    processor_tasks = [proc.s(boundary, backend) for proc in requested_processors]

    # Build the DAG
    step_setup = boundary_setup.s(boundary, backend)
    step_finalise = generate_provenance.s(boundary, backend)
    dag = chain(step_setup, group(processor_tasks), step_finalise)()
    celery_inspector = app.control.inspect()
    print (celery_inspector.active())

    # Do some collection of status during execution
    from time import sleep
    
    while dag.state != 'SUCCESS':
        print (dag.state)
        print (dag.parent.completed_count()) # Number of completed tasks in the GROUP
        print (celery_inspector.active())
        # print (celery_inspector.scheduled())
        if dag.state == 'FAILURE':
            break
        sleep(0.5)
    print ('END CHAIN RESULT (generate_provenance): ', dag.get())
    print ('PROCESSOR GROUP RESULTS: ', dag.parent.get())
    print ('WERE ALL GROUP RESULTS SUCCESSFUL?: ', dag.parent.successful())
    print ('Boundary result:', dag.parent.parent.get())
    
