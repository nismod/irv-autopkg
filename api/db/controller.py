"""
DB Controller - Postgres
"""

import json
from typing import List
from fastapi.logger import logger

from config import LOG_LEVEL
from api.db import database
from api import schemas
from api.db.queries import Queries
from .models import Boundary

logger.setLevel(LOG_LEVEL)


class DBController:
    def __init__(self):
        pass

    async def get_all_boundary_summaries(self) -> List[schemas.BoundarySummary]:
        """
        Retrieve summary info about all boundaries
        """
        boundaries = await Queries(database).get_all_boundary_summaries()
        return [schemas.BoundarySummary.from_orm(boundary) for boundary in boundaries]

    async def _postprocess_boundary(self, boundary: Boundary) -> schemas.Boundary:
        """
        Generate Boundary detail object from an orm object 
            which includes a converted geojsoin geom
        """
        return schemas.Boundary(
            id=boundary.id,
            name=boundary.name,
            name_long=boundary.name_long,
            admin_level=boundary.admin_level,
            geometry=json.loads(boundary.ST_AsGeoJSON_1),
            envelope=json.loads(boundary.ST_AsGeoJSON_2)
        )

    async def get_boundary_by_name(self, name: str) -> schemas.Boundary:
        """
        Retrieve detail about a specific named boundary
        """
        boundary = await Queries(database).get_boundary_by_name(name)
        return await self._postprocess_boundary(boundary)

    async def search_boundaries_by_coordinates(self, latitude: float, longitude: float) -> List[schemas.Boundary]:
        """
        Get summary information about boundaries intersecting a specific coordinate
        """
        boundaries = await Queries(database).search_boundaries_by_coordinates(latitude, longitude)
        return [schemas.BoundarySummary.from_orm(boundary) for boundary in boundaries]

    async def search_boundaries_by_name(self, name: str) -> List[schemas.Boundary]:
        """
        Get summary information about boundaries with a name similar to the given
        """
        boundaries = await Queries(database).search_boundaries_by_name(name)
        return [schemas.BoundarySummary.from_orm(boundary) for boundary in boundaries]
