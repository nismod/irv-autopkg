"""
API Routes
"""

# ROUTES
API_VERSION = "v1"
API_ROUTE_BASE = f"/{API_VERSION}"

LIVENESS_ROUTE = API_ROUTE_BASE + "/liveness"
READINESS_ROUTE = API_ROUTE_BASE + "/readiness"

# Retrieval of Boundaries
BOUNDARIES_BASE_ROUTE = API_ROUTE_BASE + "/boundaries"

# Retrieval of information about availabel processors
PROCESSORS_BASE_ROUTE = API_ROUTE_BASE + "/processors"

# Retrieval of information about packages
PACKAGES_BASE_ROUTE = API_ROUTE_BASE + "/packages"
PACKAGE_ROUTE = PACKAGES_BASE_ROUTE + "/{package_id}"

# Processing Jobs associated with packages
JOBS_BASE_ROUTE = PACKAGE_ROUTE + "/jobs"
JOB_STATUS_ROUTE = JOBS_BASE_ROUTE + "/{job_id}"
