"""
Helper methods / classes
"""
import inspect
from typing import List
from types import ModuleType
import os
import requests
import zipfile
import json
from subprocess import check_output

from dataproc.processors.internal.base import BaseProcessorABC, BaseMetadataABC
from dataproc import Boundary, DataPackageLicense, DataPackageResource
from dataproc.exceptions import FileCreationException, SourceRasterProjectionException

# DAGs and Processing


def processor_name(dataset: str, version: str) -> str:
    """Generate a processor name from a dataset and version"""
    return f"{dataset}.{version}"


def dataset_name_from_processor(processor_name_version: str) -> str:
    """Generate a dataset name from a processor name ane version"""
    return processor_name_version.split(".")[0]


def valid_processor(name: str, processor: BaseProcessorABC) -> bool:
    """Check if a Processor is valid and can be used"""
    if name in ["_module", "pkgutil"]:
        return False
    # Skip top level modules without metadata
    if not hasattr(processor, "Metadata") and not hasattr(processor, "Processor"):
        return False
    if isinstance(processor, ModuleType):
        return True
    return False


def version_name_from_file(filename: str):
    """
    Generate a version from the name of a processors version file
    """
    return os.path.basename(filename).replace(".py", "")

def processor_name_from_file(filename: str):
    """
    Generate a processor from the name of the folder in-which the processor file resides
    """
    return os.path.basename(os.path.dirname(filename))

def build_processor_name_version(processor_base_name: str, version: str) -> str:
    """Build a full processor name from name and version"""
    return f"{processor_base_name}.{version}"


def list_processors() -> List[BaseProcessorABC]:
    """Retrieve a list of available processors and their versions"""
    # Iterate through Core processors and collect metadata
    import dataproc.processors.core as available_processors

    valid_processors = {}  # {name: [versions]}
    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        # Split name and version
        proc_name, proc_version = name.split(".")
        if proc_name in valid_processors.keys():
            valid_processors[proc_name].append(proc_version)
        else:
            valid_processors[proc_name] = [proc_version]
    return valid_processors


def get_processor_by_name(processor_name_version: str) -> BaseProcessorABC:
    """Retrieve a processor module by its name (including version) and check its validity"""
    import dataproc.processors.core as available_processors

    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        if name == processor_name_version:
            return processor.Processor


def get_processor_meta_by_name(processor_name_version: str) -> BaseProcessorABC:
    """Retrieve a processor MetaData module by its name (including version)"""
    import dataproc.processors.core as available_processors

    for name, processor in inspect.getmembers(available_processors):
        # Check validity
        if not valid_processor(name, processor):
            continue
        if name == processor_name_version:
            return processor.Metadata


# METADATA


def add_license_to_datapackage(
    dp_license: DataPackageLicense, datapackage: dict
) -> dict:
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
    dp_resource: DataPackageResource, datapackage: dict
) -> dict:
    """
    Append a resource (dataset) to the datapackage resources array

    ::returns datapackage dict Update with given license if applicable
    """
    # Generate the resource object
    if not "resources" in datapackage.keys():
        datapackage["resources"] = [dp_resource.asdict()]
    else:
        if not "-".join(
            [dp_resource.asdict()["name"], dp_resource.asdict()["version"]]
        ) in ["-".join([i["name"], i["version"]]) for i in datapackage["resources"]]:
            datapackage["resources"].append(dp_resource.asdict())
    return datapackage


def datapackage_resource(
    metadata: BaseMetadataABC,
    uris: List[str],
    dataset_format: str,
    dataset_sizes_bytes: List[int],
    dataset_hashes: List[str],
) -> DataPackageResource:
    """
    Generate a datapackage resource for this processor

    ::param output_fpath str Local path to the processed data used to generate the hash
    ::uris List[str] Final URIs of the output data (on storage backend).
    """
    return DataPackageResource(
        name=metadata.name,
        version=metadata.version,
        path=uris,
        description=metadata.description,
        dataset_format=dataset_format,
        dataset_size_bytes=dataset_sizes_bytes,
        sources=[metadata.data_origin_url],
        dp_license=metadata.data_license,
        dataset_hashes=dataset_hashes,
    ).asdict()


def data_file_hash(fpath: str) -> str:
    """
    Generate a sha256 hash of a single datafile
    """
    _hash = (
        check_output(f"openssl sha1 {fpath}".split(" "))
        .decode()
        .replace("\n", "")
        .split("= ")[1]
    )
    return _hash


def data_file_size(fpath: str) -> int:
    """Filesize in bytes"""
    return os.path.getsize(fpath)


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
    # urllib.request.urlretrieve(source_url, path)
    response = requests.get(
        source_url,
        timeout=5,
        stream=True,
        headers={
            "Accept": "application/zip",
            "Accept-Encoding": "gzip",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
        },
    )
    with open(destination_fpath, "wb") as handle:
        for data in response.iter_content(chunk_size=8192):
            handle.write(data)
    if not os.path.exists(destination_fpath):
        raise FileCreationException()
    else:
        return destination_fpath


# RASTER OPERATIONS


def assert_geotiff(fpath: str, check_crs: str = "EPSG:4326", check_compression=True):
    """
    Check a given file is a valid geotiff

    ::param fpath str Absolute filepath
    """
    import rasterio

    with rasterio.open(fpath) as src:
        assert (
            src.meta["crs"] == check_crs
        ), f"raster CRS {src.meta['crs']} doesnt not match expected {check_crs}"
        if check_compression is True:
            assert src.compression is not None, "raster did not have any compression"


def crop_raster(
    raster_input_fpath: str,
    raster_output_fpath: str,
    boundary: Boundary,
    preserve_raster_crs=False,
) -> bool:
    """
    Crop a raster file to the given boundary (EPSG:4326)

    Generates a geotiff.

    __NOTE__ if the input raster CRS is not EPSG:4326 the boundary will be rep

    ::param raster_input_fpath str Absolute Filepath of input
    ::param raster_output_fpath str Absolute Filepath of output
    ::kwarg preserve_raster_crs bool If True the source raster CRS will be preserved in the result
        (input boundary will be reprojected to source CRS before clip)
    """

    import rasterio
    import rasterio.mask
    import shapely
    from shapely.ops import transform
    import pyproj

    # Create the path to output if it doesnt exist
    os.makedirs(os.path.dirname(raster_output_fpath), exist_ok=True)
    shape = shapely.from_geojson(json.dumps(boundary["envelope_geojson"]))
    with rasterio.open(raster_input_fpath) as src:
        # Project the source boundary () to source raster if requested output is to match source raster CRS
        source_raster_epsg = ":".join(src.crs.to_authority())
        if preserve_raster_crs is True:
            source_boundary_crs = pyproj.CRS("EPSG:4326")
            target_boundary_crs = pyproj.CRS(source_raster_epsg)

            project = pyproj.Transformer.from_crs(
                source_boundary_crs, target_boundary_crs, always_xy=True
            ).transform
            shape = transform(project, shape)
        else:
            # Abort if source raster is not matching 4326
            if source_raster_epsg != "EPSG:4326":
                raise SourceRasterProjectionException(
                    f"Aborting unknown reproject - Source raster is {source_raster_epsg} and preserve_raster_crs is False"
                )

        out_image, out_transform = rasterio.mask.mask(src, [shape], crop=True)
        out_meta = src.meta

    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
        }
    )

    with rasterio.open(
        raster_output_fpath, "w", **out_meta, compress="PACKBITS"
    ) as dest:
        dest.write(out_image)

        return os.path.exists(raster_output_fpath)


# VECTOR OPERATIONS


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
