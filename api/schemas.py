"""
Pydantic Schemas
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, ConfigDict, RootModel, AnyUrl, EmailStr, Field, constr


class Polygon(RootModel):
    """Reference to the external GeoJSON Polygon JSON Schema"""

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            schema.clear()
            schema["$ref"] = "https://geojson.org/schema/Polygon.json"


class MultiPolygon(RootModel):
    """Reference to the external GeoJSON MultiPolygon JSON Schema"""

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            schema.clear()
            schema["$ref"] = "https://geojson.org/schema/MultiPolygon.json"


class DataPackage(RootModel):
    """Reference to the external DataPackage JSON Schema"""

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            schema.clear()
            schema["$ref"] = (
                "https://specs.frictionlessdata.io/schemas/data-package.json"
            )


class BoundarySummary(BaseModel):
    """Summary of a boundary"""

    id = int
    name: str
    name_long: str

    model_config = ConfigDict(from_attributes=True)


class Boundary(BoundarySummary):
    """Complete boundary information"""

    admin_level: str
    geometry: MultiPolygon
    envelope: Polygon

    model_config = ConfigDict(from_attributes=True)


class PackageSummary(BaseModel):
    """Summary information about a top-level package (which is formed from a boundary)"""

    boundary_name: str  # Name of the Boundary the package was created from
    uri: str  # URI to the package


class Package(PackageSummary):
    """Detailed information about a package"""

    datapackage: DataPackage


class Contributor(BaseModel):
    title: str = Field(
        ...,
        description="A human-readable title.",
        examples=['{\n  "title": "My Package Title"\n}\n'],
        title="Title",
    )
    path: Optional[constr(regex=r"^(?=^[^./~])(^((?!\.{2}).)*$).*$")] = Field(
        None,
        description="A fully qualified URL, or a POSIX file path.",
        examples=[
            '{\n  "path": "file.csv"\n}\n',
            '{\n  "path": "http://example.com/file.csv"\n}\n',
        ],
        title="Path",
    )
    email: Optional[EmailStr] = Field(
        None,
        description="An email address.",
        examples=['{\n  "email": "example@example.com"\n}\n'],
        title="Email",
    )
    organization: Optional[str] = Field(
        None,
        description="An organizational affiliation for this contributor.",
        title="Organization",
    )
    role: Optional[str] = "contributor"


class Licenses(BaseModel):
    name: constr(regex=r"^([-a-zA-Z0-9._])+$") = Field(
        ...,
        description="MUST be an Open Definition license identifier, see http://licenses.opendefinition.org/",
        title="Open Definition license identifier",
    )
    path: Optional[constr(regex=r"^(?=^[^./~])(^((?!\.{2}).)*$).*$")] = Field(
        None,
        description="A fully qualified URL, or a POSIX file path.",
        examples=[
            '{\n  "path": "file.csv"\n}\n',
            '{\n  "path": "http://example.com/file.csv"\n}\n',
        ],
        title="Path",
    )
    title: Optional[str] = Field(
        None,
        description="A human-readable title.",
        examples=['{\n  "title": "My Package Title"\n}\n'],
        title="Title",
    )


class PathItem(BaseModel):
    __root__: constr(regex=r"^(?=^[^./~])(^((?!\.{2}).)*$).*$") = Field(
        ...,
        description="A fully qualified URL, or a POSIX file path.",
        examples=[
            '{\n  "path": "file.csv"\n}\n',
            '{\n  "path": "http://example.com/file.csv"\n}\n',
        ],
        title="Path",
    )


class Source(BaseModel):
    title: str = Field(
        ...,
        description="A human-readable title.",
        examples=['{\n  "title": "My Package Title"\n}\n'],
        title="Title",
    )
    path: Optional[constr(regex=r"^(?=^[^./~])(^((?!\.{2}).)*$).*$")] = Field(
        None,
        description="A fully qualified URL, or a POSIX file path.",
        examples=[
            '{\n  "path": "file.csv"\n}\n',
            '{\n  "path": "http://example.com/file.csv"\n}\n',
        ],
        title="Path",
    )
    email: Optional[EmailStr] = Field(
        None,
        description="An email address.",
        examples=['{\n  "email": "example@example.com"\n}\n'],
        title="Email",
    )


class Resources(BaseModel):
    profile: Optional[str] = Field(
        "data-resource",
        description="The profile of this descriptor.",
        examples=[
            '{\n  "profile": "tabular-data-package"\n}\n',
            '{\n  "profile": "http://example.com/my-profiles-json-schema.json"\n}\n',
        ],
        title="Profile",
    )
    name: constr(regex=r"^([-a-z0-9._/])+$") = Field(
        ...,
        description="An identifier string. Lower case characters with `.`, `_`, `-` and `/` are allowed.",
        examples=['{\n  "name": "my-nice-name"\n}\n'],
        title="Name",
    )
    path: Optional[
        Union[constr(regex=r"^(?=^[^./~])(^((?!\.{2}).)*$).*$"), List[PathItem]]
    ] = Field(
        None,
        description="A reference to the data for this resource, as either a path as a string, or an array of paths as strings. of valid URIs.",
        examples=[
            '{\n  "path": [\n    "file.csv",\n    "file2.csv"\n  ]\n}\n',
            '{\n  "path": [\n    "http://example.com/file.csv",\n    "http://example.com/file2.csv"\n  ]\n}\n',
            '{\n  "path": "http://example.com/file.csv"\n}\n',
        ],
        title="Path",
    )
    data: Any = Field(..., description="Inline data for this resource.", title="Data")
    schema_: Optional[Union[str, Dict[str, Any]]] = Field(
        None, alias="schema", description="A schema for this resource.", title="Schema"
    )
    title: Optional[str] = Field(
        None,
        description="A human-readable title.",
        examples=['{\n  "title": "My Package Title"\n}\n'],
        title="Title",
    )
    description: Optional[str] = Field(
        None,
        description="A text description. Markdown is encouraged.",
        examples=[
            '{\n  "description": "# My Package description\\nAll about my package."\n}\n'
        ],
        title="Description",
    )
    homepage: Optional[AnyUrl] = Field(
        None,
        description="The home on the web that is related to this data package.",
        examples=['{\n  "homepage": "http://example.com/"\n}\n'],
        title="Home Page",
    )
    sources: Optional[List[Source]] = Field(
        None,
        description="The raw sources for this resource.",
        examples=[
            '{\n  "sources": [\n    {\n      "title": "World Bank and OECD",\n      "path": "http://data.worldbank.org/indicator/NY.GDP.MKTP.CD"\n    }\n  ]\n}\n'
        ],
        min_items=0,
        title="Sources",
    )
    licenses: Optional[List[Licenses]] = Field(
        None,
        description="The license(s) under which the resource is published.",
        examples=[
            '{\n  "licenses": [\n    {\n      "name": "odc-pddl-1.0",\n      "path": "http://opendatacommons.org/licenses/pddl/",\n      "title": "Open Data Commons Public Domain Dedication and License v1.0"\n    }\n  ]\n}\n'
        ],
        min_items=1,
        title="Licenses",
    )
    format: Optional[str] = Field(
        None,
        description="The file format of this resource.",
        examples=['{\n  "format": "xls"\n}\n'],
        title="Format",
    )
    mediatype: Optional[constr(regex=r"^(.+)/(.+)$")] = Field(
        None,
        description="The media type of this resource. Can be any valid media type listed with [IANA](https://www.iana.org/assignments/media-types/media-types.xhtml).",
        examples=['{\n  "mediatype": "text/csv"\n}\n'],
        title="Media Type",
    )
    encoding: Optional[str] = Field(
        "utf-8",
        description="The file encoding of this resource.",
        examples=['{\n  "encoding": "utf-8"\n}\n'],
        title="Encoding",
    )
    bytes: Optional[int] = Field(
        None,
        description="The size of this resource in bytes.",
        examples=['{\n  "bytes": 2082\n}\n'],
        title="Bytes",
    )
    hash: Optional[constr(regex=r"^([^:]+:[a-fA-F0-9]+|[a-fA-F0-9]{32}|)$")] = Field(
        None,
        description="The MD5 hash of this resource. Indicate other hashing algorithms with the {algorithm}:{hash} format.",
        examples=[
            '{\n  "hash": "d25c9c77f588f5dc32059d2da1136c02"\n}\n',
            '{\n  "hash": "SHA256:5262f12512590031bbcc9a430452bfd75c2791ad6771320bb4b5728bfb78c4d0"\n}\n',
        ],
        title="Hash",
    )


class DataPackage(BaseModel):
    profile: Optional[str] = Field(
        "data-package",
        description="The profile of this descriptor.",
        examples=[
            '{\n  "profile": "tabular-data-package"\n}\n',
            '{\n  "profile": "http://example.com/my-profiles-json-schema.json"\n}\n',
        ],
        title="Profile",
    )
    name: Optional[constr(regex=r"^([-a-z0-9._/])+$")] = Field(
        None,
        description="An identifier string. Lower case characters with `.`, `_`, `-` and `/` are allowed.",
        examples=['{\n  "name": "my-nice-name"\n}\n'],
        title="Name",
    )
    id: Optional[str] = Field(
        None,
        description="A property reserved for globally unique identifiers. Examples of identifiers that are unique include UUIDs and DOIs.",
        examples=[
            '{\n  "id": "b03ec84-77fd-4270-813b-0c698943f7ce"\n}\n',
            '{\n  "id": "http://dx.doi.org/10.1594/PANGAEA.726855"\n}\n',
        ],
        title="ID",
    )
    title: Optional[str] = Field(
        None,
        description="A human-readable title.",
        examples=['{\n  "title": "My Package Title"\n}\n'],
        title="Title",
    )
    description: Optional[str] = Field(
        None,
        description="A text description. Markdown is encouraged.",
        examples=[
            '{\n  "description": "# My Package description\\nAll about my package."\n}\n'
        ],
        title="Description",
    )
    homepage: Optional[AnyUrl] = Field(
        None,
        description="The home on the web that is related to this data package.",
        examples=['{\n  "homepage": "http://example.com/"\n}\n'],
        title="Home Page",
    )
    created: Optional[datetime] = Field(
        None,
        description="The datetime on which this descriptor was created.",
        examples=['{\n  "created": "1985-04-12T23:20:50.52Z"\n}\n'],
        title="Created",
    )
    contributors: Optional[List[Contributor]] = Field(
        None,
        description="The contributors to this descriptor.",
        examples=[
            '{\n  "contributors": [\n    {\n      "title": "Joe Bloggs"\n    }\n  ]\n}\n',
            '{\n  "contributors": [\n    {\n      "title": "Joe Bloggs",\n      "email": "joe@example.com",\n      "role": "author"\n    }\n  ]\n}\n',
        ],
        min_items=1,
        title="Contributors",
    )
    keywords: Optional[List[str]] = Field(
        None,
        description="A list of keywords that describe this package.",
        examples=[
            '{\n  "keywords": [\n    "data",\n    "fiscal",\n    "transparency"\n  ]\n}\n'
        ],
        min_items=1,
        title="Keywords",
    )
    image: Optional[str] = Field(
        None,
        description="A image to represent this package.",
        examples=[
            '{\n  "image": "http://example.com/image.jpg"\n}\n',
            '{\n  "image": "relative/to/image.jpg"\n}\n',
        ],
        title="Image",
    )
    licenses: Optional[List[Licenses]] = Field(
        None,
        description="The license(s) under which this package is published.",
        examples=[
            '{\n  "licenses": [\n    {\n      "name": "odc-pddl-1.0",\n      "path": "http://opendatacommons.org/licenses/pddl/",\n      "title": "Open Data Commons Public Domain Dedication and License v1.0"\n    }\n  ]\n}\n'
        ],
        min_items=1,
        title="Licenses",
    )
    resources: List[Resources] = Field(
        ...,
        description="An `array` of Data Resource objects, each compliant with the [Data Resource](/data-resource/) specification.",
        examples=[
            '{\n  "resources": [\n    {\n      "name": "my-data",\n      "data": [\n        "data.csv"\n      ],\n      "mediatype": "text/csv"\n    }\n  ]\n}\n'
        ],
        min_items=1,
        title="Data Resources",
    )
    sources: Optional[List[Source]] = Field(
        None,
        description="The raw sources for this resource.",
        examples=[
            '{\n  "sources": [\n    {\n      "title": "World Bank and OECD",\n      "path": "http://data.worldbank.org/indicator/NY.GDP.MKTP.CD"\n    }\n  ]\n}\n'
        ],
        min_items=0,
        title="Sources",
    )
