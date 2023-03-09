
from dataclasses import dataclass
from typing import List

class Boundary(dict):
    """Encapsulates the definition of a boundary required for downstream processing"""

    def __init__(self, name: str, geojson: dict, envelope_geojson: dict):
        dict.__init__(self, name=name, geojson=geojson, envelope_geojson=envelope_geojson)
        self.name = name
        self.geojson = geojson
        self.envelope_geojson = envelope_geojson

@dataclass(frozen=True, eq=True)
class DataPackageLicense:
    """
    Datapackage License. See: https://specs.frictionlessdata.io//data-package/#licenses

    name: The name MUST be an Open Definition license ID
    path: A url-or-path string, that is a fully qualified HTTP address, or a relative POSIX path (see the url-or-path definition in Data Resource for details).
    title: A human-readable title.

    Frozen to enable hashing in dataproc
    
    """
    name: str = ""
    path: str = ""
    title: str = ""

    def asdict(self):
        return {
            "name": self.name,
            "path": self.path,
            "title": self.title
        }

@dataclass(frozen=True, eq=True)
class DataPackageResource:
    """
    A Resource entry for a datapackage - there is a one to one mapping
        between resources included in the datapackage a each dataset-version

    """
    name: str
    version: str
    path: List[str]
    description: str
    dataset_format: str
    dataset_size_bytes: int
    dataset_hashes: List[str]
    sources: List[dict]
    dp_license: DataPackageLicense

    def asdict(self):
        return {
            "name": self.name,
            "version": self.version,
            "path": self.path,
            "description": self.description,
            "format": self.dataset_format,
            "bytes": self.dataset_size_bytes,
            "hashes": self.dataset_hashes,
            "license": self.dp_license.asdict(),
            "sources": self.sources
        }