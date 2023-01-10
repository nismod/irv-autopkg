"""
Test Helpers
"""
import os
import sys
import inspect
import json
from typing import Tuple
from math import floor

import sqlalchemy as sa
import rasterio

from config import get_db_uri_sync, API_DB_NAME, INTEGRATION_TEST_ENDPOINT
from api import db

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

test_data_dir = os.path.join(current_dir, "data")

db_uri = get_db_uri_sync(API_DB_NAME)
# Init DB and Load via SA
engine = sa.create_engine(db_uri, pool_pre_ping=True)


def wipe_db():
    """Wipe all SQLA Tables in the DB"""
    print("Running wipe db...")
    db_uri = get_db_uri_sync(API_DB_NAME)
    # Init DB and Load via SA
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    for tbl in reversed(db.Base.metadata.sorted_tables):
        engine.execute(tbl.delete())
    print("Wipe db has run")


def build_route(postfix_url: str):
    return "{}{}".format(INTEGRATION_TEST_ENDPOINT, postfix_url)


def load_country_geojson(name: str) -> Tuple[dict, dict]:
    """
    Load the geojson boundary and envelope for a given country
    """
    with open(os.path.join(test_data_dir, "countries", f"{name}.geojson"), "r") as fptr:
        boundary = json.load(fptr)

    with open(
        os.path.join(test_data_dir, "countries", f"{name}_envelope.geojson"), "r"
    ) as fptr:
        envelope = json.load(fptr)

    return boundary, envelope


def assert_raster_bounds_correct(
    raster_fpath: str, envelope: dict, tolerence: float = 0.1
):
    """
    Check the bounds of the given raster match the given envelope (almost)

    ::param envelope dict Geojson Dict of boundary envelope (Polygon)
    """
    x_coords = [i[0] for i in envelope["coordinates"][0]]
    y_coords = [i[1] for i in envelope["coordinates"][0]]
    with rasterio.open(raster_fpath) as src:
        assert (
            abs(src.bounds.left - min(x_coords)) < tolerence
        ), f"bounds {src.bounds.left} did not match expected {min(x_coords)} within tolerence {tolerence}"
        assert (
            abs(src.bounds.right - max(x_coords)) < tolerence
        ), f"bounds {src.bounds.right} did not match expected {max(x_coords)} within tolerence {tolerence}"
        assert (
            abs(src.bounds.top - max(y_coords)) < tolerence
        ), f"bounds {src.bounds.top} did not match expected {max(y_coords)} within tolerence {tolerence}"
        assert (
            abs(src.bounds.bottom - min(y_coords)) < tolerence
        ), f"bounds {src.bounds.bottom} did not match expected {min(y_coords)} within tolerence {tolerence}"
