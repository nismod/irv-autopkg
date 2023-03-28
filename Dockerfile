FROM osgeo/gdal:ubuntu-small-3.6.2

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# copy project
WORKDIR /usr/src/app

# install dependencies and add user
RUN apt-get update && \ 
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    addgroup -gid 1002 autopkg && adduser --system --disabled-login -uid 1002 --gid 1002 autopkg

# Load Pip deps as Autopkg
COPY requirements.txt .
USER autopkg
RUN pip3 install --user --no-cache --upgrade --no-warn-script-location pip -r requirements.txt

# Load App and alter user
USER root
COPY config.py .
COPY api ./api
COPY dataproc ./dataproc
COPY tests ./tests
RUN mkdir -p /usr/src/app/tests/data/processing && mkdir /usr/src/app/tests/data/packages && chown -R autopkg:autopkg /usr/src/app

USER autopkg
# Make sure scripts in .local are usable:
ENV PATH=/home/autopkg/.local/bin:$PATH
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app/"

# Run unit tests
RUN python3 -m unittest /usr/src/app/tests/dataproc/unit/processors/test_env.py
