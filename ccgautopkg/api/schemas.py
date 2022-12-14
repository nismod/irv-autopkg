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