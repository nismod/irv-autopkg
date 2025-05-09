FROM python:3.12
LABEL maintainer="frederick.thomas@ouce.ox.ac.uk"

WORKDIR /code

# Install project with dependencies - these can be cached as docker layers if unchanged
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

# copy project
COPY config.py .
COPY api ./api
COPY dataproc ./dataproc
COPY tests ./tests

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
