"""
API Helpers
"""

import traceback


def handle_exception(logger, err: Exception):
    """
    Handle generic exceptions
    """
    logger.error(
        "%s failed with tb %s, error: %s",
        __name__,
        traceback.format_tb(err.__traceback__),
        err,
    )


def build_package_url(packages_host_url: str, boundary_name: str) -> str:
    """Build the url to top-level package directory for a given boundary"""
    return f"{packages_host_url}/{boundary_name}/"


def build_dataset_version_url(
    packages_host_url: str, boundary_name: str, dataset_name: str, dataset_version: str
) -> str:
    """Build the url a dataset version directory for a given boundary"""
    return f"{packages_host_url}/{boundary_name}/datasets/{dataset_name}/{dataset_version}/"
