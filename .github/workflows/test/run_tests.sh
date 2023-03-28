#!/bin/sh
pip install pytest && \
echo Executing API Tests... && \
ls -lrt /usr/src/app/tests/data && \
pytest -p no:warnings -v /usr/src/app/tests/api && \
echo Executing Processor Tests... && \
pytest -p no:warnings -v /usr/src/app/tests/dataproc
