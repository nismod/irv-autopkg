"""
Test Helpers
"""
import os
import sys
import inspect
import json
from typing import Any, List, Tuple
import shutil

import sqlalchemy as sa
import rasterio
import shapely
from shapely.ops import transform
import pyproj

from config import get_db_uri_sync, API_POSTGRES_DB, INTEGRATION_TEST_ENDPOINT
from api import db

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

test_data_dir = os.path.join(current_dir, "data")

db_uri = get_db_uri_sync(API_POSTGRES_DB)
# Init DB and Load via SA
engine = sa.create_engine(db_uri, pool_pre_ping=True)


def wipe_db():
    """Wipe all SQLA Tables in the DB"""
    print("Running wipe db...")
    db_uri = get_db_uri_sync(API_POSTGRES_DB)
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


def load_natural_earth_roads_to_pg():
    """
    Load the natrual earth shapefile of roads into Postgres
    Enables testing of vector clipping Processor
    """
    pguri = str(get_db_uri_sync(API_POSTGRES_DB)).replace("+psycopg2", "")
    fpath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "data",
        "global",
        "ne_10m_roads",
        "ne_10m_roads.shp",
    )
    cmd = f'ogr2ogr -f "PostgreSQL" -nlt PROMOTE_TO_MULTI PG:"{pguri}" "{fpath}"'
    os.system(cmd)


def drop_natural_earth_roads_from_pg():
    """Drop loaded Natural Earth Roads data from DB"""
    print("Dropping NE Test Roads from DB...")
    db_uri = get_db_uri_sync(API_POSTGRES_DB)
    # Init DB and Load via SA
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    _ = engine.execute("DROP TABLE ne_10m_roads;")
    print("Dropped NE Test Roads")


def gen_datapackage(boundary_name: str, dataset_names: List[str]) -> dict:
    """A test datapackage"""
    dp_license = {
        "name": "ODbL-1.0",
        "path": "https://opendefinition.org/licenses/odc-odbl",
        "title": "Open Data Commons Open Database License 1.0",
    }
    return {
        "name": boundary_name,
        "title": boundary_name,
        "profile": "data-package",
        "licenses": [dp_license for _ in dataset_names],
        "resources": [
            {
                "name": dataset_name,
                "version": "version_1",
                "path": ["data.gpkg"],
                "description": "desc",
                "format": "GEOPKG",
                "bytes": ["d7bbfe3d26e2142ee24458df087ed154194fe5de"],
                "hashes": [22786048],
                "license": dp_license,
                "sources": ["a url"],
            }
            for dataset_name in dataset_names
        ],
    }


def create_tree(
    top_level_path: str,
    packages: list = ["gambia", "zambia"],
    datasets: list = ["aqueduct", "biodiversity", "osm_roads"],
):
    """
    Create a fake tree so we can check reading packages
    """
    # Generate the datapackage.jsons
    for package in packages:
        os.makedirs(os.path.join(top_level_path, package), exist_ok=True)
        dp = gen_datapackage(package, datasets)
        with open(
            os.path.join(top_level_path, package, "datapackage.json"), "w"
        ) as fptr:
            json.dump(dp, fptr)
    if "gambia" in packages:
        if "noexist" in datasets:
            # An invalid processor or dataset was placed in the tree
            os.makedirs(
                os.path.join(top_level_path, "gambia", "datasets", "noexist"),
                exist_ok=True,
            )
        if "aqueduct" in datasets:
            os.makedirs(
                os.path.join(top_level_path, "gambia", "datasets", "aqueduct", "0.1"),
                exist_ok=True,
            )
        if "biodiversity" in datasets:
            os.makedirs(
                os.path.join(
                    top_level_path, "gambia", "datasets", "biodiversity", "version_1"
                ),
                exist_ok=True,
            )
        if "osm_roads" in datasets:
            os.makedirs(
                os.path.join(
                    top_level_path, "gambia", "datasets", "osm_roads", "20221201"
                ),
                exist_ok=True,
            )
        if "natural_earth_raster" in datasets:
            os.makedirs(
                os.path.join(
                    top_level_path,
                    "gambia",
                    "datasets",
                    "natural_earth_raster",
                    "version_1",
                ),
                exist_ok=True,
            )
    if "zambia" in packages:
        if "osm_roads" in datasets:
            os.makedirs(
                os.path.join(
                    top_level_path, "zambia", "datasets", "osm_roads", "20230401"
                ),
                exist_ok=True,
            )


def remove_tree(top_level_path: str, packages=["gambia", "zambia"]):
    """
    Cleanup the test tree
    """
    for package in packages:
        try:
            shutil.rmtree(os.path.join(top_level_path, package))
        except FileNotFoundError:
            print(
                "warning - failed to remove package at:",
                os.path.join(top_level_path, package),
                "file not found",
            )


def assert_raster_bounds_correct(
    raster_fpath: str, envelope: dict, tolerence: float = 0.1
):
    """
    Check the bounds of the given raster match the given envelope (almost)

    ::param envelope dict Geojson Dict of boundary envelope (Polygon)
    """
    with rasterio.open(raster_fpath) as src:
        # Reproject bounds as necessary based on the source raster
        source_raster_epsg = ":".join(src.crs.to_authority())
        if source_raster_epsg != "EPSG:4326":
            shape = shapely.from_geojson(json.dumps(envelope))
            source_boundary_crs = pyproj.CRS("EPSG:4326")
            target_boundary_crs = pyproj.CRS(source_raster_epsg)

            project = pyproj.Transformer.from_crs(
                source_boundary_crs, target_boundary_crs, always_xy=True
            ).transform
            shape = transform(project, shape)
            x_coords, y_coords = shape.exterior.coords.xy
            tolerence = 1000.0
        else:
            x_coords = [i[0] for i in envelope["coordinates"][0]]
            y_coords = [i[1] for i in envelope["coordinates"][0]]
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


def assert_package(
    top_level_fpath: str, boundary_name: str, dataset_name_versions: List[str]
):
    """Assert integrity of a package and datasets contained within
        This does not assert the integrity of actualy data files (raster/vector);
        just the folder structure

    ::param dataset_name_versions str name.version
    """
    required_top_level_docs = [
        "index.html",
        "license.html",
        "version.html",
        "provenance.json",
        "datapackage.json",
    ]
    packages = next(os.walk(top_level_fpath))[1]
    assert (
        boundary_name in packages
    ), f"{boundary_name} missing in package root: {packages}"
    for dataset in next(
        os.walk(os.path.join(top_level_fpath, boundary_name, "datasets"))
    )[1]:
        for version in next(
            os.walk(os.path.join(top_level_fpath, boundary_name, "datasets", dataset))
        )[1]:
            chk_path = os.path.join(
                top_level_fpath, boundary_name, "datasets", dataset, version
            )
            assert os.path.exists(chk_path), f"missing files in pacakge: {chk_path}"
    # Ensure the top-level index and other docs exist
    for doc in required_top_level_docs:
        assert os.path.exists(
            os.path.join(top_level_fpath, boundary_name, doc)
        ), f"top-level {doc} missing"


def assert_table_in_pg(db_uri: str, tablename: str):
    """Check a given table exists in PG"""
    from sqlalchemy.sql import text

    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    stmt = text(f'SELECT * FROM "{tablename}"')
    engine.execute(stmt)


def setup_test_data_paths(processor: Any, test_processing_data_dir: str):
    """
    Reset the processing paths on an instantiated processor module to reflect the test environment
    """
    processor.paths_helper.top_level_folder_path = test_processing_data_dir
    processor.source_folder = processor.paths_helper.build_absolute_path("source_data")
    processor.tmp_processing_folder = processor.paths_helper.build_absolute_path("tmp")


def assert_datapackage_resource(dp_resource: dict):
    """
    Check if the given resource of a datapckage appear valid
    See: https://specs.frictionlessdata.io//data-package
    """
    assert "path" in dp_resource.keys(), "datapackage missing path"
    assert "name" in dp_resource.keys(), "datapackage missing name"
    assert isinstance(dp_resource["path"], list), "datapackage path not a list"
    assert isinstance(dp_resource["hashes"], list), "datapackage hashes not a list"
    assert isinstance(dp_resource["bytes"], list), "datapackage bytes not a list"
    assert (
        len(dp_resource["path"])
        == len(dp_resource["hashes"])
        == len(dp_resource["bytes"])
    ), f"datapackage path, hashes and bytes must be the same length {len(dp_resource['path'])}, {len(dp_resource['hashes'])}, {len(dp_resource['bytes'])}"
    assert isinstance(dp_resource["license"], dict), "datapackage license must be dict"
    assert (
        "name" in dp_resource["license"].keys()
    ), "datapackage license must include name"
    assert (
        "path" in dp_resource["license"].keys()
    ), "datapackage license must include path"
    assert (
        "title" in dp_resource["license"].keys()
    ), "datapackage license must include title"
