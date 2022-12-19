"""
Clean and load Boundaries GEOJSON
"""

import sys
import json
import string
import os
import inspect
import asyncio
from typing import List, Tuple

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

from api.config import get_db_uri_sync, API_DB_NAME
from api.db.models import Boundary


def clean_name(name: str) -> str:
    """
    Remove unwanted chars from a name
    """
    name = name.replace(" ", "-")
    name = name.lower()
    valid_chars = list(string.ascii_lowercase)
    valid_chars.append("-")
    return "".join(x for x in name if x in valid_chars)


def load_boundaries_json(
    boundaries_geojson_fpath: str, accepted_crs=["EPSG:4326"], name_column="name"
) -> dict:
    """
    Load Boundaries Geojson to PostGIS
    """
    with open(boundaries_geojson_fpath, "r") as fptr:
        data = json.load(fptr)
    # Check EPSG
    if not data["crs"]["properties"]["name"] in accepted_crs:
        raise Exception(
            f"Boundary Features must be one of the following CRS's: {accepted_crs}"
        )
    # Ensure all features have clean names
    for feature in data["features"]:
        if name_column not in feature["properties"].keys():
            raise Exception(
                f"Boundary Feature GeoJSON properties must contain unique name field, {feature} does not"
            )
        feature["properties"][name_column] = clean_name(
            feature["properties"][name_column]
        )
    return data


def load_boundaries(
    boundaries_geojson_fpath: str,
    name_column="name",
    long_name_column="name_long",
    admin_level="0",
) -> Tuple[bool, List]:
    """
    Load a geojson file of multipolygons into Boundaries table
    Internally converts Polygons to Multi
    """
    data = load_boundaries_json(boundaries_geojson_fpath)
    db_uri = get_db_uri_sync(API_DB_NAME)
    # Init DB and Load via SA
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    loaded_ids = []
    with Session() as session:
        try:
            for feature in data["features"]:
                boundary = Boundary(
                    name=feature["properties"][name_column],
                    name_long=feature["properties"][long_name_column],
                    admin_level=admin_level,
                    geometry=sa.func.ST_AsEWKT(
                        sa.func.ST_SetSRID(
                            sa.func.ST_Multi(
                                sa.func.ST_GeomFromGeoJSON(
                                    json.dumps(feature["geometry"])
                                )
                            ),
                            4326,
                        )
                    ),
                )
                session.add(boundary)
                _id = session.commit()
                loaded_ids.append(_id)
        except Exception as err:
            print(f"Boundary insert failed due to {err}, rolling back transaction...")
            session.rollback()
    engine.dispose()
    success = len(loaded_ids) == len(data["features"])
    return success, loaded_ids


if __name__ == "__main__":
    fpath = sys.argv[1]
    if not fpath:
        print("Usage:", "load_boundaries.py <geojson file path>")
        sys.exit(1)
    all_loaded, ids = load_boundaries_json(fpath)
    print(f"Loaded {len(ids)} boundary features, success: {all_loaded}")
