# FROM python:3.10-slim

# # set work directory
# WORKDIR /usr/src/app

# # set environment variables
# ENV PYTHONDONTWRITEBYTECODE 1
# ENV PYTHONUNBUFFERED 1

# # install dependencies
# COPY ./requirements.txt .
# RUN apt-get update && apt-get install -y software-properties-common && \
#     add-apt-repository ppa:ubuntugis/ppa && apt-get update && apt-get install -y \
#     python3-numpy python3-numpy gdal-bin libgdal-dev && \
#     pip install --upgrade pip && \
#     pip install -r requirements.txt

# # copy project
# COPY . .

FROM osgeo/gdal:alpine-small-3.6.2

# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
COPY ./requirements.txt .
RUN apk add --virtual .build-deps \
        --repository http://dl-cdn.alpinelinux.org/alpine/edge/community \
        --repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
        gcc libc-dev geos-dev geos && \
    apk add --update --no-cache python3 python3-dev alpine-sdk && ln -sf python3 /usr/bin/python && \
    python3 -m ensurepip && \
    pip3 install --no-cache --upgrade pip setuptools && \
    pip3 install -r requirements.txt

# copy project
COPY . .
