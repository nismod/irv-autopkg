#!/bin/sh
pip install --user pytest && \
echo Executing API Tests... && \
pytest -p no:warnings -v --junitxml=/opt/test_results/pytest.xml /usr/src/app/tests/api
# echo Executing Processor Tests... && \
# pytest -p no:warnings -v --junitxml=/opt/test_results/pytest.xml /usr/src/app/tests/dataproc
