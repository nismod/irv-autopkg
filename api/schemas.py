"""
Pydantic Schemas
"""

from typing import List, Optional

from pydantic import BaseModel, validator

class GeoJSON(BaseModel):
     """Reference to the external GeoJSON JSON Schema"""
     __root__: dict

     class Config:
         @staticmethod
         def schema_extra(schema: dict):
             schema.clear()
             schema["$ref"] = "https://geojson.org/schema/GeoJSON.json"

class DataPackage(BaseModel):
    """Reference to the external DataPackage JSON Schema"""
    __root__: dict

    class Config:
        @staticmethod
        def schema_extra(schema: dict):
            schema.clear()
            schema["$ref"] = "https://specs.frictionlessdata.io/schemas/data-package.json"

class BoundarySummary(BaseModel):
    """Summary of a boundary"""
    id=int
    name:str
    name_long:str

    class Config:
        orm_mode = True

class Boundary(BoundarySummary):
    """Complete boundary information"""
    admin_level:str
    geometry:GeoJSON
    envelope:GeoJSON

    class Config:
        orm_mode = True

class ProcessorMetadata(BaseModel):
    """Detail about a Data Processor"""
    name: str
    description: str
    dataset: str
    author: str
    license: dict
    origin_url: str
    version: str
    status: Optional[str]=""

class ProcessorVersion(BaseModel):
    """A Version of a Processor"""
    version: str
    processor: ProcessorMetadata # Metadata about the versioned processor which created this dataset
    uri: Optional[str]=""

class Processor(BaseModel):
    """Summary information about a Processor"""
    name: str # Name of the processor
    versions: List[ProcessorVersion] # Versions of the processor, which are created by versioned processors of the same name

class PackageSummary(BaseModel):
    """Summary information about a top-level package (which is formed from a boundary)"""
    boundary_name: str # Name of the Boundary the package was created from
    uri: str # URI to the package

class Package(PackageSummary):
    """Detailed information about a package"""
    boundary: Boundary # Boundary from-which the package has been created
    processors: List[Processor] # Datasets within this package
    datapackage: DataPackage # Datapackage.json parsed from the FS and nested within the Package response

# Jobs

class Job(BaseModel):
    boundary_name: str
    processors: List[str] # List of processor names

    @validator('processors')
    def no_dups(cls, v):
        if len(set(v)) != len(v):
            raise ValueError('duplicate processors not allowed')
        return v

class SubmittedJob(BaseModel):
    """A successfully submitted Job"""
    job_id: str

class JobProgress(BaseModel):
    """
    Specifics about the progress of an individual Processors Job
    """
    percent_complete: Optional[int]=0
    current_task: Optional[str]

class JobStatus(SubmittedJob):
    """Status of a Submitted Job"""
    processor_name: str
    job_status: str
    job_progress: Optional[JobProgress]
    job_result: Optional[dict]

class JobGroupStatus(BaseModel):
    """
    Status of the Processor Group in a submited DAG
    """
    job_group_status: str
    job_group_percent_complete: Optional[int]=0
    job_group_processors: List[JobStatus]