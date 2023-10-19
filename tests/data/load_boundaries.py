"""
Clean and load Boundaries GEOJSON
"""

import sys
import json
import string
import os
from typing import List, Tuple

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

from config import get_db_uri_sync, API_POSTGRES_DB
from api.db.models import Boundary, Base


def clean_name(name: str) -> str:
    """
    Remove unwanted chars from a name
    """
    name = name.replace(" ", "-")
    name = name.lower()
    valid_chars = list(string.ascii_lowercase)
    return "".join(x for x in name if x in valid_chars)


def load_boundaries_json(
    boundaries_geojson_fpath: str,
    accepted_crs=["EPSG:4326"],
    name_column="name",
    skip_names=["-99"],
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
        if feature["properties"][name_column] in skip_names:
            continue
        feature["properties"][name_column] = clean_name(
            feature["properties"][name_column]
        )
    return data


def load_boundaries(
    boundaries_geojson_fpath: str,
    name_column="name",
    long_name_column="name_long",
    admin_level="0",
    wipe_table=True,
    skip_names=["-99"],
    setup_tables=True,
) -> Tuple[bool, List]:
    """
    Load a geojson file of multipolygons into Boundaries table
    Internally converts Polygons to Multi
    """
    data = load_boundaries_json(boundaries_geojson_fpath, name_column=name_column)
    db_uri = get_db_uri_sync(API_POSTGRES_DB)
    # Init DB and Load via SA
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    if setup_tables is True:
        Base.metadata.create_all(engine)
    if wipe_table is True:
        for tbl in reversed(Base.metadata.sorted_tables):
            engine.execute(tbl.delete())
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    loaded_ids = []
    skipped = 0
    with Session() as session:
        try:
            for feature in data["features"]:
                if feature["properties"][name_column] in skip_names:
                    print("skipped boundary:", feature["properties"][name_column])
                    skipped += 1
                    continue
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
    success = len(loaded_ids) == (len(data["features"]) - skipped)
    return success, loaded_ids


if __name__ == "__main__":
    if not len(sys.argv) == 5:
        print(
            "Usage:",
            "load_boundaries.py <geojson file path> <name_column> <long_name_column> <wipe table true/false>",
        )
        sys.exit(1)
    fpath = sys.argv[1]
    name_column = sys.argv[2]
    long_name_column = sys.argv[3]
    wipe_table = sys.argv[4]
    if not fpath:
        print("missing fpath")
        sys.exit(1)
    if not name_column:
        print("no name_column supplied - using name")
        name_column = "name"
    if not long_name_column:
        print("no long_name_column supplied - using long_name")
        name_column = "long_name"
    if wipe_table == "true":
        wipe_table = True
    else:
        wipe_table = False
    print("Loading with: ", fpath, name_column, long_name_column, wipe_table)
    all_loaded, ids = load_boundaries(
        fpath,
        name_column=name_column,
        long_name_column=long_name_column,
        wipe_table=wipe_table,
    )
    print(f"Loaded {len(ids)} boundary features, success: {all_loaded}")
