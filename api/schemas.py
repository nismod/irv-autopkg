"""Pydantic Schemas
"""

from typing import List, Optional
from enum import Enum

from pydantic import BaseModel, validator

from dataproc.helpers import processors_as_enum
from config import INCLUDE_TEST_PROCESSORS

MISSING_PROC_MSG = "processor details not available"


class Polygon(BaseModel):
    """Reference to the external GeoJSON Polygon JSON Schema"""

    __root__: dict

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            schema.clear()
            schema["$ref"] = "https://geojson.org/schema/Polygon.json"


class MultiPolygon(BaseModel):
    """Reference to the external GeoJSON MultiPolygon JSON Schema"""

    __root__: dict

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            schema.clear()
            schema["$ref"] = "https://geojson.org/schema/MultiPolygon.json"


class DataPackage(BaseModel):
    """Reference to the external DataPackage JSON Schema"""

    __root__: dict

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            schema.clear()
            schema[
                "$ref"
            ] = "https://specs.frictionlessdata.io/schemas/data-package.json"


class BoundarySummary(BaseModel):
    """Summary of a boundary"""

    id = int
    name: str
    name_long: str

    class Config:
        orm_mode = True


class Boundary(BoundarySummary):
    """Complete boundary information"""

    admin_level: str
    geometry: MultiPolygon
    envelope: Polygon

    class Config:
        orm_mode = True


class ProcessorVersionMetadata(BaseModel):
    """Detail about a Data Processor"""

    name: str
    description: str
    version: str
    status: Optional[str] = ""  # Used while executing
    uri: Optional[str] = ""  # Used when package is available
    data_author: str
    data_title: str
    data_title_long: str
    data_summary: str
    data_citation: str
    data_license: dict
    data_origin_url: str
    data_formats: List[str]


class Processor(BaseModel):
    """Summary information about a Processor"""

    name: str  # Name of the processor
    versions: List[
        ProcessorVersionMetadata
    ]  # Versions of the processor, which are created by versioned processors of the same name


class PackageSummary(BaseModel):
    """Summary information about a top-level package (which is formed from a boundary)"""

    boundary_name: str  # Name of the Boundary the package was created from
    uri: str  # URI to the package


class Package(PackageSummary):
    """Detailed information about a package"""

    boundary: Boundary  # Boundary from-which the package has been created
    processors: List[Processor]  # Datasets within this package
    datapackage: DataPackage  # Datapackage.json parsed from the FS and nested within the Package response


#
# Jobs
#
class Job(BaseModel):
    boundary_name: str
    processors: List[str]  # List of processor names

    @validator("processors")
    def no_dups(cls, v):
        if len(set(v)) != len(v):
            raise ValueError("duplicate processors not allowed")
        return v


class SubmittedJob(BaseModel):
    """A successfully submitted Job"""

    job_id: str


class JobProgress(BaseModel):
    """
    Specifics about the progress of an individual Processors Job
    """

    percent_complete: Optional[int] = 0
    current_task: Optional[str]


class JobStateEnum(str, Enum):
    """Possible Job States"""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    EXECUTING = "EXECUTING"
    RETRY = "RETRY"
    SKIPPED = "SKIPPED"
    REVOKED = "REVOKED"


class JobStatus(SubmittedJob):
    """Status of a Submitted Job"""

    processor_name: processors_as_enum(
        include_test_processors=INCLUDE_TEST_PROCESSORS, additions=[MISSING_PROC_MSG]
    )
    job_status: JobStateEnum
    job_progress: Optional[JobProgress]
    job_result: Optional[dict]

    class Config:
        use_enum_values = True


class JobGroupStatus(BaseModel):
    """
    Status of the Processor Group in a submited DAG
    """

    job_group_status: str
    job_group_percent_complete: Optional[int] = 0
    job_group_processors: List[JobStatus]
