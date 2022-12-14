# AutoPackaging GRI Datasets

FastAPI + Celery + Individual processors per Dataset for DAG ETL Pipeline

## API

#### PG Schema Management with Alembic

* Make changes as required to models
* From within the ccgautoppkg/api folder run the following to auto-generate an upgrade/downgrade script:

```bash
alembic revision --autogenerate -m "Added Boundary Table"
```

__NOTE__: CHECK the script - remove extransous operations (in particular those relating to spatial-ref-sys)

* When reaady run the following to upgrade the database:

```bash
alembic upgrade head
```