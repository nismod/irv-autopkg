"""
API Routes
"""

# ROUTES
API_VERSION = "v1"
API_ROUTE_BASE = f"/{API_VERSION}"

# Retrieval of all Boundaries (no detail)
BOUNDARIES_BASE_ROUTE = API_ROUTE_BASE + "/boundaries"
BOUNDARY_ROUTE = BOUNDARIES_BASE_ROUTE + "/{name}"

# Retrieval of information about packages
PACKAGES_BASE_ROUTE = API_ROUTE_BASE + "/packages"
PACKAGE_ROUTE = PACKAGES_BASE_ROUTE + "/{boundary_name}"
