"""
Helper methods / classes
"""
import csv
import inspect
import json
import os
import shutil
import warnings
import zipfile
from collections import defaultdict, OrderedDict
from enum import Enum
from subprocess import check_output, CalledProcessError, check_call
from time import time
from types import ModuleType
from typing import Dict, Generator, List, Optional, Tuple

import numpy as np
import rasterio
import requests
from rasterio import sample

from dataproc import Boundary, DataPackageLicense, DataPackageResource
from dataproc.exceptions import (
    FileCreationException,
    SourceRasterProjectionException,
    UnexpectedFilesException,
    ZenodoGetFailedException,
)

# DAGs and Processing


def processors_as_enum(
    include_test_processors: bool = False, additions: List[str] = []
) -> Enum:
    """Generate an Enum of the currently available processors"""
    procs = {}
    for proc_name, proc_versions in list_processors(
        include_test_processors=include_test_processors
    ).items():
        for version in proc_versions:
            name_version = build_processor_name_version(proc_name, version)
            procs[name_version] = name_version
    # Add in any additional fields
    for addition in additions:
        if not addition in procs.keys():
            procs[addition] = addition
    return Enum("ProcessorsEnum", procs)


def processor_name(dataset: str, version: str) -> str:
    """Generate a processor name from a dataset and version"""
    return f"{dataset}.{version}"


def dataset_name_from_processor(processor_name_version: str) -> str:
    """Generate a dataset name from a processor name ane version"""
    return processor_name_version.split(".")[0]


def valid_processor(name: str, processor) -> bool:
    """Check if a Processor is valid and can be used"""
    if name in ["_module", "pkgutil"]:
        return False
    # Skip top level modules without metadata
    if not hasattr(processor, "Metadata") and not hasattr(processor, "Processor"):
        return False
    if isinstance(processor, ModuleType):
        return True
    return False


def build_processor_name_version(processor_base_name: str, version: str) -> str:
    """Build a full processor name from name and version"""
    return f"{processor_base_name}.{version}"


def list_processors(
    include_test_processors: bool = False,
) -> Dict[str, List[str]]:
    """Retrieve a list of available processors and their versions"""
    # Iterate through Core processors and collect metadata
    import dataproc.processors.core as available_processors

    valid_processors: Dict[str, List[str]] = defaultdict(list)  # {name: [versions]}
    for name, processor in inspect.getmembers(available_processors):
        if include_test_processors is False:
            if "test" in name:
                continue
        # Check validity
        if valid_processor(name, processor) is False:
            continue
        # Split name and version
        proc_name, proc_version = name.split(".")
        valid_processors[proc_name].append(proc_version)
    return valid_processors


def get_processor_by_name(processor_name_version: str):
    """Retrieve a processor module by its name (including version) and check its validity"""
    import dataproc.processors.core as available_processors

    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        if name == processor_name_version:
            return processor.Processor
    return None


def get_processor_meta_by_name(processor_name_version: str):
    """Retrieve a processor MetaData module by its name (including version)"""
    import dataproc.processors.core as available_processors

    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        if name == processor_name_version:
            return processor.Metadata
    return None


# METADATA


def add_license_to_datapackage(
    dp_license: DataPackageLicense, datapackage: Dict
) -> Dict:
    """
    Append a license to the datapackage licenses array

    ::returns datapackage dict Update with given license if applicable
    """
    if not "licenses" in datapackage.keys():
        datapackage["licenses"] = [dp_license.asdict()]
    else:
        if not dp_license.asdict()["name"] in [
            i["name"] for i in datapackage["licenses"]
        ]:
            datapackage["licenses"].append(dp_license.asdict())
    return datapackage


def add_dataset_to_datapackage(
    dp_resource: DataPackageResource, datapackage: Dict
) -> Dict:
    """
    Append a resource (dataset) to the datapackage resources array

    ::returns datapackage dict Updated with given dataset if applicable
    """
    # Generate the resource object
    if not "resources" in datapackage.keys():
        datapackage["resources"] = [dp_resource.asdict()]
    else:
        if not "-".join(
            [dp_resource.asdict()["name"], dp_resource.asdict()["version"]]
        ) in ["-".join([i["name"], i["version"]]) for i in datapackage["resources"]]:
            datapackage["resources"].append(dp_resource.asdict())
    # Update the license
    datapackage = add_license_to_datapackage(dp_resource.dp_license, datapackage)
    return datapackage


def datapackage_resource(
    metadata,
    uris: List[str],
    dataset_format: str,
    dataset_sizes_bytes: List[int],
    dataset_hashes: List[str],
) -> DataPackageResource:
    """
    Generate a datapackage resource for this processor

    ::uris List[str] Final URIs of the output data (on storage backend).
    """
    return DataPackageResource(
        name=metadata.name,
        version=metadata.version,
        path=uris,
        description=metadata.description,
        dataset_format=dataset_format,
        dataset_size_bytes=sum(dataset_sizes_bytes),
        sources=[{"title": metadata.dataset_name, "path": metadata.data_origin_url}],
        dp_license=metadata.data_license,
        dataset_hashes=dataset_hashes,
    )


def data_file_hash(fpath: str) -> str:
    """Generate the SHA-1 hash of a single file"""
    _hash = (
        check_output(f"openssl sha1 {fpath}".split(" "))
        .decode()
        .replace("\n", "")
        .split("= ")[1]
    )
    return _hash


# FILE OPERATIONS


def unpack_zip(zip_fpath: str, target_folder: str):
    """
    Unpack a Downloaded Zip

    ::param zip_fpath str Absolute Filepath of input
    ::param target_folder str Zip content will be extracted to the given folder

    ::returns extracted folder path str
    """
    os.makedirs(target_folder, exist_ok=True)
    with zipfile.ZipFile(zip_fpath, "r") as zip_ref:
        zip_ref.extractall(target_folder)


def create_test_file(fpath: str):
    """
    Generate a blank test-file
    """
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    with open(fpath, "w") as fptr:
        fptr.write("test\n")


def download_file(source_url: str, destination_fpath: str) -> str:
    """
    Download a file from a source URL to a given destination

    Folders to the path will be created as required
    """
    os.makedirs(os.path.dirname(destination_fpath), exist_ok=True)
    with requests.get(
        source_url,
        timeout=5,
        stream=True,
        headers={
            "Accept": "application/zip",
            "Accept-Encoding": "gzip",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
        },
    ) as r:
        with open(destination_fpath, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    if not os.path.exists(destination_fpath):
        raise FileCreationException()
    else:
        return destination_fpath


def tiffs_in_folder(
    folder_path: str, basename: str = "", full_paths: bool = False
) -> List[str]:
    """
    Return the filenames of all tiffs in a given folder

    ::kwarg basename str If set this will filter tiff filenames by the given string
    ::kwargs full_paths bool Return full file paths instead of just filenames
    """
    files = [
        i
        for i in os.listdir(folder_path)
        if basename in os.path.splitext(i)[0] and os.path.splitext(i)[1] == ".tif"
    ]
    if full_paths:
        return [os.path.join(folder_path, _file) for _file in files]
    return files


def unpack_and_check_zip_tifs(
    local_zip_fpath: str,
    target_folder: str,
    expected_crs: str,
    num_expected_tifs: int = 1,
    expected_hashes: List[str] = None,
) -> List[str]:
    """
    Unpack a downloaded zip and do some checks to ensure it is correct

    ::kwarg expected_hash str If present the hash of the file will be calculated and checked.
        The ordering of hashes is not checked - just the presence of all matching hashes.

    ::return tif_fpath List[str] Paths to the contained zip(s)
    """
    unpack_zip(local_zip_fpath, target_folder)
    # Filter on basename in case we're un[packing to an existing folder with tiffs]
    unpacked_files = tiffs_in_folder(
        target_folder, basename=os.path.splitext(os.path.basename(local_zip_fpath))[0]
    )
    if len(unpacked_files) != num_expected_tifs:
        raise UnexpectedFilesException(
            f"Source zip {local_zip_fpath} has unexpected number of tifs: {unpacked_files}"
        )
    output_tifs = []
    extracted_file_hashes = []
    for tif in unpacked_files:
        source_tif_fpath = os.path.join(target_folder, tif)
        # Ensure the tif is valid
        assert_geotiff(
            source_tif_fpath, check_crs=expected_crs, check_compression=False
        )
        # Collect hashes if requested
        if expected_hashes is not None:
            _hash = data_file_hash(source_tif_fpath)
            extracted_file_hashes.append(_hash)
        output_tifs.append(source_tif_fpath)
    # Check hashes if required
    if expected_hashes is not None:
        if not sorted(extracted_file_hashes) == sorted(expected_hashes):
            raise UnexpectedFilesException(
                f"Downloaded file hashes {sorted(extracted_file_hashes)} did not match expected: {sorted(expected_hashes)}"
            )
    return output_tifs


def fetch_zenodo_doi(
    doi: str, target_folder: str, return_only_tifs: bool = True
) -> List[str]:
    """
    Fetch source files associated with a given DOI.

    Data will be downloaded and unpacked into target_folder.

    MD5 Sums optionally checked after download

    ::kwarg return_only_tifs bool Remove non-tif files from the return file-list

    ::returns output_file_paths List[str] List of all resulting files
    """
    cmd = ["zenodo_get", "-d", doi, "-o", target_folder]
    try:
        check_call(cmd)
    except CalledProcessError:
        raise ZenodoGetFailedException(
            f"zenodo_get cmd {cmd} failed with non-zero exit code"
        )
    if return_only_tifs:
        return [
            os.path.join(target_folder, _file)
            for _file in tiffs_in_folder(target_folder)
        ]
    return [
        os.path.join(target_folder, _file.name) for _file in os.scandir(target_folder)
    ]


# RASTER OPERATIONS
def is_bigtiff(filename):
    """
    https://stackoverflow.com/questions/60427572/how-to-determine-if-a-tiff-was-written-in-bigtiff-format
    """
    import struct

    with open(filename, "rb") as f:
        header = f.read(4)
    byteorder = {b"II": "<", b"MM": ">", b"EP": "<"}[header[:2]]
    version = struct.unpack(byteorder + "H", header[2:4])[0]
    return version == 43


def sample_geotiff_coords(fpath: str, num_coords: int = 10) -> np.ndarray:
    """
    Retrieve a set of coordinates within the bounds of the given raster
    """
    with rasterio.open(fpath, "r") as src:
        return np.column_stack(
            (
                np.random.uniform(
                    low=src.bounds.left, high=src.bounds.right, size=(num_coords,)
                ),
                np.random.uniform(
                    low=src.bounds.bottom, high=src.bounds.top, size=(num_coords,)
                ),
            )
        )


def sample_geotiff(
    fpath: str, coords: np.ndarray = None, num_samples: int = 10
) -> Tuple[np.ndarray, List[np.ndarray]]:
    """
    Retrieve a sample of given GeoTIFF file.

    Optionally provide coords from-which to sample pixels. (2d np array of coordinates within the raster bounds)

    If not provided coords will be sampled at random from the raster bounds

    ::returns Tuple (coords, samples (shape = num_samplesx(np.array(pixel per band))))
    """
    if coords is None:
        # Take a random sample of coords within bounds
        coords = sample_geotiff_coords(fpath, num_samples)
    with rasterio.open(fpath, "r") as src:
        samples = sample.sample_gen(src, coords)
        return coords, [sample for sample in samples]


def assert_geotiff(
    fpath: str,
    check_crs: str = "EPSG:4326",
    check_compression=True,
    check_is_bigtiff=False,
    check_pixel_coords: np.ndarray = None,
    check_pixel_expected_samples: List[np.ndarray] = None,
):
    """
    Check a given file is a valid geotiff, optionally checking:
        Coordinate Reference System Match
        Compression exists (Any)
        The TIFF has BIGTIFF tags
        The TIFF pixels match some expected data samples at given coordinates

    ::param fpath str Absolute filepath
    """
    with rasterio.open(fpath, "r") as src:
        if check_crs is not None:
            assert (
                src.meta["crs"] == check_crs
            ), f"raster CRS {src.meta['crs']} doesnt not match expected {check_crs}"

        if check_compression is True:
            assert src.compression is not None, "raster did not have any compression"

        if check_pixel_coords is not None and check_pixel_expected_samples is not None:
            src_samples = sample.sample_gen(src, check_pixel_coords)
            for idx, src_sample in enumerate(src_samples):
                # Special case for nan comparison
                if all(np.isnan(src_sample)) and all(
                    np.isnan(check_pixel_expected_samples[idx])
                ):
                    continue
                assert (
                    np.array_equal(src_sample, check_pixel_expected_samples[idx])
                    is True
                ), f"source pixels did not match expected pixel samples at coords: {check_pixel_coords[idx]}, {src_sample} != {check_pixel_expected_samples[idx]}"

    if check_is_bigtiff is True:
        assert (
            is_bigtiff(fpath) is True
        ), f"raster is not a bigtiff when it was expected to be: {fpath}"


def crop_raster(
    raster_input_fpath: str,
    raster_output_fpath: str,
    boundary: Boundary,
    creation_options=["COMPRESS=PACKBITS"],
    debug=False,
) -> bool:
    """
    Crop a raster using GDAL translate
    """
    from osgeo import gdal
    import shapely
    import pyproj
    from shapely.ops import transform
    import shlex
    import subprocess

    # # Gather the resolution
    inds = gdal.Open(raster_input_fpath)

    source_boundary_crs = pyproj.CRS("EPSG:4326")
    target_boundary_crs = pyproj.crs.CRS.from_wkt(inds.GetProjection())
    if source_boundary_crs != target_boundary_crs:
        # Reproject boundary to source raster for projwin
        project = pyproj.Transformer.from_crs(
            source_boundary_crs, target_boundary_crs, always_xy=True
        ).transform
        inshape = shapely.from_geojson(json.dumps(boundary["envelope_geojson"]))
        shape = transform(project, inshape)
        bounds = shape.bounds
    else:
        shape = shapely.from_geojson(json.dumps(boundary["envelope_geojson"]))
        bounds = shape.bounds

    gdal_translate = shutil.which("gdal_translate")
    if not gdal_translate:
        raise Exception("gdal_translate not found")
    cmd = f"{gdal_translate} -projwin {bounds[0]} {bounds[3]} {bounds[2]} {bounds[1]} {raster_input_fpath} {raster_output_fpath}"
    # Add Creation Options
    for creation_option in creation_options:
        cmd = cmd + f" -co {creation_option}"
    if debug is True:
        print("Raster Crop Command:", cmd)

    result = subprocess.run(shlex.split(cmd), capture_output=True)
    if debug is True:
        print("Raster Crop Result:", result)
    return os.path.exists(raster_output_fpath)


# VECTOR OPERATIONS


def assert_vector_file(
    fpath: str, expected_shape: tuple = None, expected_crs: str = None
):
    """
    Check a given file is a valid vector file and can beparsed with geopandas.

    Optionally assert the data shape and CRS authority string

    ::arg fpath str Absolute filepath
    ::kwarg expected_crs str CRS with authority - e.g. "EPSG:4326"
    """
    import fiona

    with fiona.open(fpath, "r") as fptr:
        if expected_shape is not None:
            shape = (
                len(fptr),
                len(fptr.schema["properties"].keys()) + 1,
            )  # Add geom col to count of cols
            assert (
                shape == expected_shape
            ), f"shape did not match expected: {shape}, {expected_shape}"
        if expected_crs is not None:
            crs = ":".join(fptr.crs.to_authority())
            assert (
                crs == expected_crs
            ), f"crs did not match expected: {crs}, {expected_crs}"


def ogr2ogr_load_shapefile_to_pg(shapefile_fpath: str, pg_uri: str):
    """
    Load a shapefile into Postgres - uses system OGR command

    Uses the ogr2ogr -nlt PROMOTE_TO_MULTI flat
    """
    cmd = f'ogr2ogr -f "PostgreSQL" -nlt PROMOTE_TO_MULTI PG:"{pg_uri}" "{shapefile_fpath}"'
    os.system(cmd)


def gpkg_layer_name(pg_table_name: str, boundary: Boundary) -> str:
    """
    Derive an output name for the GeoPKG from the pg table and boundary
    """
    return f"{pg_table_name}_{boundary['name']}"


def copy_from_pg_table(pg_uri: str, sql: str, output_csv_fpath: str) -> int:
    """
    Execute a COPY FROM for the given pg uri and SQL statement

    ::returns filesize int
    """
    import psycopg2

    sql = f"""COPY ({sql}) TO STDOUT WITH CSV HEADER"""
    with psycopg2.connect(dsn=pg_uri) as conn:
        with open(output_csv_fpath, "w") as fptr:
            with conn.cursor() as cur:
                cur.copy_expert(sql, fptr)
    with open(output_csv_fpath, "rb") as fptr:
        total_lines = sum(1 for i in fptr) - 1  # Remove header line
    return total_lines


def crop_osm_to_geopkg(
    boundary: Boundary,
    pg_uri: str,
    pg_table: str,
    output_fpath: str,
    geometry_column: str = "geom",
    extract_type: str = "clip",
    limit: int = None,
    batch_size: int = 1000,
) -> Generator:
    """
    Uses GDAL interface to crop table to geopkg
    https://gis.stackexchange.com/questions/397023/issue-to-convert-from-postgresql-input-to-gpkg-using-python-gdal-api-function-gd

    GEOPKG Supports only a single Geometry column per table: https://github.com/opengeospatial/geopackage/issues/77

    __NOTE__: We assume the input and output CRS is 4326

    __NOTE__: PG doesnt permit EXCEPT syntax with field selection,
        so all fields will be output (minus the original geometries if using "clip")

    ::kwarg extract_type str
        Either "intersect" - keep the entire intersecting feature in the output
        or "clip" includes only the clipped geometry in the output

    ::returns Generator[int, int, int, int]
        Progress yield: csv_line_count, current_idx, lines_success, lines_skipped, lines_failed
    """
    import fiona
    from fiona.crs import CRS
    from shapely import from_wkt, to_geojson, from_wkb

    geojson = json.dumps(boundary["geojson"])
    if extract_type == "intersect":
        stmt = f"SELECT {geometry_column}, properties FROM {pg_table} WHERE ST_Intersects(ST_GeomFromGeoJSON('{geojson}'), {geometry_column})"
    else:
        # Clip - remembering the geometry inside properties is the entire geometry, not the clipped one
        stmt = f"""
            WITH clip_geom AS (
                SELECT st_geomfromgeojson(\'{geojson}\') AS geometry
            )
            SELECT (ST_Dump(ST_Intersection(clip_geom.geometry, {pg_table}.{geometry_column}))).geom AS {geometry_column}, properties
            FROM {pg_table}, clip_geom
            WHERE ST_Intersects({pg_table}.{geometry_column}, clip_geom.geometry)
        """
    if limit is not None and int(limit):
        stmt = f"{stmt} LIMIT {limit}"
    try:
        # Generate CSV using COPY command
        tmp_csv_fpath = os.path.join(os.path.dirname(output_fpath), f"{time()}_tmp.csv")

        # initialise count/index variables
        csv_line_count = copy_from_pg_table(pg_uri, stmt, tmp_csv_fpath)
        idx = 0
        lines_skipped = 0
        lines_failed = 0
        lines_success = 0

        # Load CSV to geopkg
        crs = CRS.from_epsg(4326)
        schema = {
            "geometry": "LineString",
            "properties": OrderedDict(
                {
                    "asset_id": "float:16",
                    "osm_way_id": "str",
                    "asset_type": "str",
                    "paved": "bool",
                    "material": "str",
                    "lanes": "int",
                    "_asset_type": "str",
                    "rehab_cost_USD_per_km": "float:16",
                    "sector": "str",
                    "subsector": "str",
                    "tag_bridge": "str",
                    "bridge": "bool",
                    "wkt": "str",
                }
            ),
        }
        template = {_k: None for _k, _ in schema["properties"].items()}
        with fiona.open(
            output_fpath, "w", driver="GPKG", crs=crs, schema=schema
        ) as output:
            with open(tmp_csv_fpath, newline="") as csvfile:
                reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                next(reader, None)  # Skip header
                batch = []
                for idx, row in enumerate(reader):
                    try:
                        data = json.loads(row[1])
                        outrow = {}
                        geom = from_wkb(row[0])
                        if geom.geom_type != "LineString":
                            lines_skipped += 1
                            continue
                        outrow["geometry"] = json.loads(to_geojson(geom))
                        # Null missing fields
                        outrow["properties"] = OrderedDict(template | data)
                        batch.append(outrow)
                        if len(batch) >= batch_size:
                            output.writerecords(batch)
                            output.flush()
                            lines_success += len(batch)
                            batch = []
                            yield csv_line_count, idx + 1, lines_success, lines_skipped, lines_failed
                    except Exception as err:
                        warnings.warn(f"failed to load rows to due: {err}")
                        # Attempt to load everything in the batch apart from the failed row
                        if batch:
                            for outrow in batch:
                                try:
                                    output.write(outrow)
                                    output.flush()
                                    lines_success += 1
                                except Exception as rowerr:
                                    warnings.warn(
                                        f"failed to load row: {outrow} due to {rowerr}"
                                    )
                                    lines_failed += 1
                                finally:
                                    batch = []
                # Final batch leftover
                if len(batch) > 0:
                    output.writerecords(batch)
                    lines_success += len(batch)
                    yield csv_line_count, idx + 1, lines_success, lines_skipped, lines_failed
    finally:
        # Cleanup
        if os.path.exists(tmp_csv_fpath):
            os.remove(tmp_csv_fpath)
    yield csv_line_count, idx + 1, lines_success, lines_skipped, lines_failed


def gdal_crop_pg_table_to_geopkg(
    boundary: Boundary,
    pg_uri: str,
    pg_table: str,
    output_fpath: str,
    geometry_column: str = "wkb_geometry",
    extract_type: str = "both",
    clipped_geometry_column_name: str = "clipped_geometry",
    debug=False,
) -> None:
    """
    Uses GDAL interface to crop table to geopkg
    https://gis.stackexchange.com/questions/397023/issue-to-convert-from-postgresql-input-to-gpkg-using-python-gdal-api-function-gd
    GEOPKG Supports only a single Geometry column per table: https://github.com/opengeospatial/geopackage/issues/77
    __NOTE__: We assume the input and output CRS is 4326
    __NOTE__: PG doesnt permit EXCEPT syntax with field selection,
        so all fields will be output (minus the original geometries if using "clip")
    ::kwarg extract_type str
        Either "intersect" - keep the entire intersecting feature in the output
        or "clip" - (Default) - includes only the clipped geometry in the output
        Defaults to "both"
    """
    from osgeo import gdal

    if debug:
        gdal.UseExceptions()
        gdal.SetConfigOption("CPL_DEBUG", "ON")
    geojson = json.dumps(boundary["geojson"])
    if extract_type == "intersect":
        stmt = f"SELECT * FROM {pg_table} WHERE ST_Intersects(ST_GeomFromGeoJSON('{geojson}'), {geometry_column})"
    else:
        # gdalVectorTranslate selects the first geometry column to add as the GeoPKG layer.
        # Hence we place the clipped_geometry first in the output - then other geometry layers are ignored
        stmt = f"""
            WITH clip_geom AS (
                SELECT st_geomfromgeojson(\'{geojson}\') AS geometry
            )
            SELECT (ST_Dump(ST_Intersection(clip_geom.geometry, {pg_table}.{geometry_column}))).geom AS {clipped_geometry_column_name}, *
            FROM {pg_table}, clip_geom
            WHERE ST_Intersects({pg_table}.{geometry_column}, clip_geom.geometry)
        """
    if debug:
        print("SQL TO BE EXECUTED: ", stmt)
    ds = gdal.OpenEx(pg_uri, gdal.OF_VECTOR)
    vector_options = gdal.VectorTranslateOptions(
        dstSRS="EPSG:4326",
        srcSRS="EPSG:4326",
        reproject=False,
        format="GPKG",
        SQLStatement=stmt,
        layerName=gpkg_layer_name(pg_table, boundary),
    )
    gdal.VectorTranslate(output_fpath, ds, options=vector_options)


def fiona_crop_file_to_geopkg(
    input_fpath: str,
    boundary: Boundary,
    output_fpath: str,
    output_schema: Dict,
    output_crs: int = 4326,
) -> bool:
    """
    Crop file by given boundary mask, streaming data from the given input to output GPKG.

    Intersects using Shapely.interects

    ::arg schema Fiona schema of format.  Must match input schema, e.g.:
        {
            "geometry": "LineString",
            "properties": OrderedDict(
                {
                    "asset_id": "float:16",
                    "osm_way_id": "str",
                    "asset_type": "str",
                    ...
                }
            ),
        }
    """
    import fiona
    from fiona.crs import CRS
    import shapely

    clip_geom = shapely.from_geojson(json.dumps(boundary["geojson"]))
    with fiona.open(
        output_fpath,
        "w",
        driver="GPKG",
        crs=CRS.from_epsg(output_crs),
        schema=output_schema,
    ) as fptr_output:
        with fiona.open(input_fpath) as fptr_input:
            for input_row in fptr_input:
                if shapely.geometry.shape(input_row.geometry).intersects(clip_geom):
                    fptr_output.write(input_row)
    return os.path.exists(output_fpath)


def csv_to_gpkg(
    input_csv_fpath: str,
    output_gpkg_fpath: str,
    crs: str = "EPSG:4326",
    latitude_col: str = "latitude",
    longitude_col: str = "longitude",
) -> bool:
    """
    Convert a given CSV to geopackage
    """
    import geopandas as gp
    import pandas as pd

    df = pd.read_csv(
        input_csv_fpath,
        dtype={
            "country": str,
            "country_long": str,
            "name": str,
            "gppd_idnr": str,
            "primary_fuel": str,
            "other_fuel1": str,
            "other_fuel2": str,
            "other_fuel3": str,
            "owner": str,
            "source": str,
            "url": str,
            "geolocation_source": str,
            "wepp_id": str,
            "generation_data_source": str,
            "estimated_generation_note_2013": str,
            "estimated_generation_note_2014": str,
            "estimated_generation_note_2015": str,
            "estimated_generation_note_2016": str,
            "estimated_generation_note_2017": str,
        },
    )
    if not latitude_col in df.columns or not longitude_col in df.columns:
        raise Exception(
            f"latitude and longitude columns required in CSV columns, got: {df.columns}"
        )
    df = df.set_geometry(gp.points_from_xy(df[longitude_col], df[latitude_col]))
    df.crs = crs
    df.to_file(output_gpkg_fpath)
    return os.path.exists(output_gpkg_fpath)
