"""
Pydantic Schemas
"""

from typing import List, Optional

from pydantic import BaseModel, validator

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
    geometry:dict # GeoJSON
    envelope:dict # GeoJSON

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
    datapackage: dict # Datapackage.json parsed from the FS and nested within the Package response

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

class JobStatus(SubmittedJob):
    """Status of a Submitted Job"""
    job_status: str
    job_result: Optional[dict]