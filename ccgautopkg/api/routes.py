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

# Retrieval of information about availabel processors
PROCESSORS_BASE_ROUTE = API_ROUTE_BASE + "/processors"

# Retrieval of information about packages
PACKAGES_BASE_ROUTE = API_ROUTE_BASE + "/packages"
PACKAGE_ROUTE = PACKAGES_BASE_ROUTE + "/{boundary_name}"

# Processing Jobs associated with packages
JOBS_BASE_ROUTE = API_ROUTE_BASE + "/jobs"
JOB_STATUS_ROUTE = JOBS_BASE_ROUTE + "/{job_id}"
