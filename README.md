# AutoPackaging GRI Datasets

FastAPI + Celery + Individual processors per Dataset for DAG ETL Pipeline

TODO:

Tests for Natural Earth - Bounding box of multipolygon geojson to shape rasterio can interpret

Create a processor that fetches a raster, crops it and saves it to a localfs backend

    Rasterio for raster clipping to geojson
    OGR for vector clip to geojson from a DB etc

Create a processor that fetches data from PG , crops it and dumps it to geopkg (without the PR)

Dev-env somehow

PR for integ to infra-risk (on dev branch)

S3 Backend