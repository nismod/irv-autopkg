"""
API Routes
"""

# ROUTES
API_VERSION = "v1"
API_ROUTE_BASE = f"/{API_VERSION}"

LIVENESS_ROUTE = API_ROUTE_BASE + "/liveness"
READINESS_ROUTE = API_ROUTE_BASE + "/readiness"

# Retrieval of all Boundaries (no detail)
BOUNDARIES_BASE_ROUTE = API_ROUTE_BASE + "/boundaries"
BOUNDARY_SEARCH_ROUTE = BOUNDARIES_BASE_ROUTE + "/search"
BOUNDARY_ROUTE = BOUNDARIES_BASE_ROUTE + "/{name}"

# Retrieval of information about all available processors
PROCESSORS_BASE_ROUTE = API_ROUTE_BASE + "/processors"
PROCESSORS_NAME_ROUTE = PROCESSORS_BASE_ROUTE + "/{name}"
PROCESSORS_VERSION_ROUTE = PROCESSORS_NAME_ROUTE + "/{version}"

# Retrieval of information about packages
PACKAGES_BASE_ROUTE = API_ROUTE_BASE + "/packages"
PACKAGE_ROUTE = PACKAGES_BASE_ROUTE + "/{boundary_name}"

# Processing Jobs associated with packages
JOBS_BASE_ROUTE = API_ROUTE_BASE + "/jobs"
JOB_STATUS_ROUTE = JOBS_BASE_ROUTE + "/{job_id}"

# Retrieval of GDL data and geojson
GDL_BASE_ROUTE = API_ROUTE_BASE + "/gdl"

GDL_BOUNDARY_BASE_ROUTE = GDL_BASE_ROUTE + "/geojson"
GDL_BOUNDARY_FROM_ISO_ROUTE = GDL_BOUNDARY_BASE_ROUTE + "/subnational/iso/{iso_code}"
GDL_NATIONAL_FROM_ISO_ROUTE = GDL_BOUNDARY_BASE_ROUTE + "/national/iso/{iso_code}"

GDL_META_BASE_ROUTE = GDL_BASE_ROUTE + "/meta"
GDL_COUNTRY_META_ROUTE = GDL_META_BASE_ROUTE + "/countries"
GDL_REGION_META_ROUTE = GDL_META_BASE_ROUTE + "/regions"

GDL_DATA_BASE_ROUTE = GDL_BASE_ROUTE + "/data"
GDL_DATA_ALL_ROUTE = GDL_DATA_BASE_ROUTE + "/{dataset_key}"
GDL_DATA_ISO_ROUTE = GDL_DATA_ALL_ROUTE + "/{iso_code}"
