"""
Pydantic Schemas
"""

from typing import List

from pydantic import BaseModel

class Job(BaseModel):
    boundary_id: int
    boundary_name: str
    boundary_geojson: dict
    processors: List[str]

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

    class Config:
        orm_mode = True

class ProcessorMetadata(BaseModel):
    """Detail about a Data Processor"""
    processor: str
    dataset: str
    author: str
    license: str
    origin_url: str
