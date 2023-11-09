"""
Test Helpers
"""
import os
import sys
import json
from typing import Any, List, Optional, Tuple
import shutil
from time import sleep, time

import sqlalchemy as sa
import rasterio
import shapely
from shapely.ops import transform
import pyproj
from pyarrow import fs
from pyarrow.fs import S3FileSystem

from config import get_db_uri_sync, API_POSTGRES_DB, INTEGRATION_TEST_ENDPOINT
from api.db.models import Base
from dataproc.helpers import (
    assert_geotiff,
    assert_vector_file,
    sample_geotiff,
    sample_geotiff_coords,
)
from dataproc.storage.awss3 import S3Manager, AWSS3StorageBackend

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

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
        Base.metadata.create_all(engine)  # type: ignore
    for tbl in reversed(Base.metadata.sorted_tables):  # type: ignore
        engine.execute(tbl.delete())


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
    db_uri = get_db_uri_sync(API_POSTGRES_DB)
    # Init DB and Load via SA
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    _ = engine.execute("DROP TABLE ne_10m_roads;")


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


def remove_tree_awss3(
    s3_fs: S3FileSystem, bucket: str, packages: list = ["gambia", "zambia"]
):
    """Remove a tree from aws s3 backend"""
    for package in packages:
        s3_fs.delete_dir(os.path.join(bucket, package))


def clean_packages(
    backend_type: str,
    storage_backend: Any,
    s3_bucket: Optional[str] = None,
    s3_region="eu-west-2",
    packages=["gambia"],
):
    """Remove packages used in a test"""
    max_wait = 60
    start = time()
    try:
        if backend_type == "awss3":
            assert s3_bucket is not None, "Must provide S3 bucket"
            with S3Manager(*storage_backend._parse_env(), region=s3_region) as s3_fs:
                remove_tree_awss3(s3_fs, s3_bucket, packages=packages)
            while True:
                existing_packages = storage_backend.packages()
                if any([True for i in existing_packages if i in packages]):
                    sleep(0.5)
                else:
                    break
                if (time() - start) > max_wait:
                    raise Exception("timed out waiting for packages to be deleted")
        elif backend_type == "localfs":
            remove_tree(storage_backend.top_level_folder_path, packages=packages)
        else:
            print("unknown backend type:", backend_type)
    except FileNotFoundError:
        pass


def assert_vector_output(
    expected_shape: tuple,
    expected_crs: str,
    local_vector_fpath: str = None,
    s3_fs: S3FileSystem = None,
    s3_vector_fpath: str = None,
    tmp_folder: str = None,
):
    """
    Wrapper for assert vector file with support for fetching from S3
    """
    if s3_fs and s3_vector_fpath:
        if not tmp_folder:
            local_vector_fpath = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "data",
                "processing",
                os.path.basename(s3_vector_fpath),
            )
        else:
            local_vector_fpath = os.path.join(
                tmp_folder, os.path.basename(s3_vector_fpath)
            )
        fs.copy_files(
            s3_vector_fpath,
            local_vector_fpath,
            source_filesystem=s3_fs,
            destination_filesystem=fs.LocalFileSystem(),
        )
    assert_vector_file(
        local_vector_fpath,
        expected_shape,
        expected_crs=expected_crs,
    )


def assert_raster_output(
    envelope: dict,
    localfs_raster_fpath: str = None,
    s3_fs: S3FileSystem = None,
    s3_raster_fpath: str = None,
    check_crs: str = "EPSG:4326",
    check_compression=True,
    tolerence: float = 0.1,
    tmp_folder: str = None,
    check_is_bigtiff: bool = False,
    pixel_check_raster_fpath: str = None,
    pixel_check_num_samples: int = 100,
):
    """
    Wrapper for assert_geotiff and assert_raster_bounds_correct
        which asserts either local or S3 source results
    if localfs_raster_fpath is provided then local source will be assumed

    if s3_fs and s3_raster_fpath are provided then requested source
        will be pulled locally before assertions.

    ::kwarg pixel_check_raster_fpath str
        If this kwarg is set then pixels will be sampled from the raster at localfs_raster_fpath
        and compared to pisels in the raster at pixel_check_raster_fpath
    """
    try:
        if s3_fs and s3_raster_fpath:
            if not tmp_folder:
                localfs_raster_fpath = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "data",
                    "processing",
                    os.path.basename(s3_raster_fpath),
                )
            else:
                localfs_raster_fpath = os.path.join(
                    tmp_folder, os.path.basename(s3_raster_fpath)
                )
            fs.copy_files(
                s3_raster_fpath,
                localfs_raster_fpath,
                source_filesystem=s3_fs,
                destination_filesystem=fs.LocalFileSystem(),
            )
        if pixel_check_raster_fpath is not None:
            # Collect sample and coords from the first raster, then sample second raster
            src_coords = sample_geotiff_coords(
                localfs_raster_fpath, pixel_check_num_samples
            )
            _, expected_samples = sample_geotiff(
                pixel_check_raster_fpath, coords=src_coords
            )
        else:
            src_coords = None
            expected_samples = None
        assert_geotiff(
            localfs_raster_fpath,
            check_crs=check_crs,
            check_compression=check_compression,
            check_is_bigtiff=check_is_bigtiff,
            check_pixel_coords=src_coords,
            check_pixel_expected_samples=expected_samples,
        )
        assert_raster_bounds_correct(
            localfs_raster_fpath, envelope, tolerence=tolerence
        )
    finally:
        # Clean local S3 artifacts
        if s3_fs and s3_raster_fpath:
            if os.path.exists(localfs_raster_fpath):
                os.remove(localfs_raster_fpath)


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


def assert_exists_awss3(s3_fs: S3FileSystem, s3_raster_fpath: str):
    """
    Check if a given file exists on the s3 filessytem
    """
    chk = s3_fs.get_file_info(s3_raster_fpath)
    assert (
        chk.type != fs.FileType.NotFound
    ), f"file was not found on S3 {s3_raster_fpath}"


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


def assert_package_awss3(
    awss3_backend: AWSS3StorageBackend,
    boundary_name: str,
    expected_processor_versions: List = [],
):
    """Assert integrity of a package and datasets contained within (on S3)
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
    packages = awss3_backend._list_directories(awss3_backend._build_absolute_path(""))
    assert (
        boundary_name in packages
    ), f"{boundary_name} missing in package S3 root: {packages}"

    # Ensure the top-level index and other docs exist
    for doc in required_top_level_docs:
        assert awss3_backend.boundary_file_exists(
            boundary_name, doc
        ), f"package {boundary_name} is missing a top-level file: {doc}"

    # Check we have folders for the expected processor versions
    for proc_version in expected_processor_versions:
        proc, version = proc_version.split(".")
        s3_versions = awss3_backend.dataset_versions(boundary_name, proc)
        assert (
            version in s3_versions
        ), f"{version} not found in dataset {s3_versions} for processor {proc}"


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
    processor.processing_root_folder = test_processing_data_dir
    processor.source_folder = os.path.join(
        processor.processing_root_folder, "source_data"
    )
    processor.tmp_processing_folder = os.path.join(
        processor.processing_root_folder, "tmp"
    )


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
