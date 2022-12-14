FROM python:3.10-slim AS compile-image

RUN apt-get update && \
	apt-get install -y --no-install-recommends build-essential gcc

RUN python -m venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install -r requirements.txt

FROM python:3.10-slim
RUN groupadd -g 1001 celery && \
	useradd --create-home -u 1001 -g 1001 celery &&\
	mkdir -p /var/run/celery /var/log/celery && \
	chown -R celery:celery /var/run/celery /var/log/celery
USER celery
COPY --from=compile-image --chown=celery /opt/venv /opt/venv

COPY . /usr/src/dataproc
WORKDIR /usr/src/dataproc
# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"
# Make sure scripts are usable:
ENV PYTHONPATH="${PYTHONPATH}:/usr/src/dataproc"

RUN ["python3", "-m", "unittest", "discover", "tests/unit"]

ENTRYPOINT ["celery"]
