"""
Test Helpers
"""

import os
import inspect
import json
from typing import List
import shutil

import sqlalchemy as sa
from pyarrow.fs import S3FileSystem

from config import get_db_uri_sync, API_POSTGRES_DB, INTEGRATION_TEST_ENDPOINT
from api import db
from dataproc.backends.storage.awss3 import AWSS3StorageBackend

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

test_data_dir = os.path.join(current_dir, "data")

db_uri = get_db_uri_sync(API_POSTGRES_DB)
# Init DB and Load via SA
engine = sa.create_engine(db_uri, pool_pre_ping=True)


def wipe_db(setup_tables=True):
    """Wipe all SQLA Tables in the DB"""
    db_uri = get_db_uri_sync(API_POSTGRES_DB)
    # Init DB and Load via SA
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    if setup_tables:
        db.Base.metadata.create_all(engine)
    for tbl in reversed(db.Base.metadata.sorted_tables):
        engine.execute(tbl.delete())


def build_route(postfix_url: str):
    return "{}{}".format(INTEGRATION_TEST_ENDPOINT, postfix_url)


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
        "licenses": [dp_license for _ in dataset_names],
        "resources": [
            {
                "name": dataset_name,
                "version": "version_1",
                "path": ["data.gpkg"],
                "description": "desc",
                "format": "GEOPKG",
                "hashes": ["d7bbfe3d26e2142ee24458df087ed154194fe5de"],
                "bytes": 22786048,
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
    wipe_existing: bool = True,
):
    """
    Create a fake tree in local FS so we can check reading packages
    """
    # Generate the datapackage.jsons
    for package in packages:
        if wipe_existing is True:
            shutil.rmtree(os.path.join(top_level_path, package), ignore_errors=True)
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
    Cleanup the test tree from local FS
    """
    for package in packages:
        shutil.rmtree(os.path.join(top_level_path, package), ignore_errors=True)


def create_tree_awss3(
    s3_fs: S3FileSystem,
    bucket: str,
    packages: list = ["gambia", "zambia"],
    datasets: list = ["aqueduct", "biodiversity", "osm_roads"],
    wipe_existing: bool = True,
):
    """
    Create a fake tree in local FS so we can check reading packages
    """
    # Generate the datapackage.jsons
    for package in packages:
        if wipe_existing is True:
            try:
                s3_fs.delete_dir(os.path.join(bucket, package))
            except FileNotFoundError:
                pass
        s3_fs.create_dir(os.path.join(bucket, package))
        dp = gen_datapackage(package, datasets)
        dp_fpath = os.path.join(bucket, package, "datapackage.json")
        with s3_fs.open_output_stream(dp_fpath) as stream:
            stream.write(json.dumps(dp).encode())

    if "gambia" in packages:
        if "noexist" in datasets:
            # An invalid processor or dataset was placed in the tree
            s3_fs.create_dir(os.path.join(bucket, "gambia", "datasets", "noexist"))
        if "aqueduct" in datasets:
            s3_fs.create_dir(
                os.path.join(bucket, "gambia", "datasets", "aqueduct", "0.1")
            )
        if "biodiversity" in datasets:
            s3_fs.create_dir(
                os.path.join(bucket, "gambia", "datasets", "biodiversity", "version_1")
            )
        if "osm_roads" in datasets:
            s3_fs.create_dir(
                os.path.join(bucket, "gambia", "datasets", "osm_roads", "20221201")
            )
        if "natural_earth_raster" in datasets:
            s3_fs.create_dir(
                os.path.join(
                    bucket,
                    "gambia",
                    "datasets",
                    "natural_earth_raster",
                    "version_1",
                )
            )
    if "zambia" in packages:
        if "osm_roads" in datasets:
            s3_fs.create_dir(
                os.path.join(bucket, "zambia", "datasets", "osm_roads", "20230401")
            )


def assert_package(top_level_fpath: str, boundary_name: str):
    """Assert integrity of a package and datasets contained within
    This does not assert the integrity of actualy data files (raster/vector);
    just the folder structure
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
            assert os.path.exists(chk_path), f"missing files in package: {chk_path}"
    # Ensure the top-level index and other docs exist
    for doc in required_top_level_docs:
        assert os.path.exists(
            os.path.join(top_level_fpath, boundary_name, doc)
        ), f"top-level {doc} missing"


def assert_datapackage_resource(dp_resource: dict):
    """
    Check if the given resource of a datapckage appear valid
    See: https://specs.frictionlessdata.io//data-package
    """
    assert "path" in dp_resource.keys(), "datapackage missing path"
    assert "name" in dp_resource.keys(), "datapackage missing name"
    assert isinstance(dp_resource["path"], list), "datapackage path not a list"
    assert isinstance(dp_resource["hashes"], list), "datapackage hashes not a list"
    assert isinstance(
        dp_resource["bytes"], int
    ), f"datapackage bytes {dp_resource['bytes']} not a int was {type(dp_resource['bytes'])}"
    assert len(dp_resource["path"]) == len(
        dp_resource["hashes"]
    ), f"datapackage path and hashes must be the same length {len(dp_resource['path'])}, {len(dp_resource['hashes'])}"
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
