#!/bin/sh
pip install --user pytest && \
echo Executing API Tests... && \
pytest -p no:warnings -v /usr/src/app/tests/api
# echo Executing Processor Tests... && \
# pytest -p no:warnings -v /usr/src/app/tests/dataproc
