"""
DB Queries - Postgres
"""

import json
from typing import List

from sqlalchemy.sql import select, update, insert, func
from geoalchemy2 import Geometry
from fastapi.logger import logger
from databases import Database

from config import LOG_LEVEL
from api.db import models
from api.exceptions import BoundaryNotFoundException

logger.setLevel(LOG_LEVEL)


class Queries:
    def __init__(self, database: Database):
        self.database = database

    async def geojson_to_geom(self, geojson: dict) -> str:
        """
        Generate geometry from GeoJSON via DB
        """
        geojson = json.dumps(geojson)
        stmt = select(
            func.ST_AsEWKT(func.ST_SetSRID(func.ST_GeomFromGeoJSON(geojson), 4326))
        )
        res = await self.database.execute(stmt)
        return res

    async def geom_to_geojson(self, geometry: Geometry) -> dict:
        """
        If the record contains a geometry it'll be converted to geojson
        NOTE: Assumes the geometry field is called geometry
        ::param geometry SQLAlchemy Geometry
        """
        stmt = select(func.ST_AsGeoJSON(geometry))
        res = await self.database.execute(stmt)
        # Format to object
        return json.loads(res)

    async def get_boundary_by_name(self, name: str) -> models.Boundary:
        """
        Get detailed information about a specific boundary
            This includes a GeoJSON repr of the geometry under field ST_AsGeoJSON
        """
        stmt = select(
            models.Boundary,
            func.ST_AsGeoJSON(models.Boundary.geometry),
            func.ST_AsGeoJSON(func.ST_Envelope(models.Boundary.geometry)),
        ).where(models.Boundary.name == name)
        res = await self.database.fetch_one(stmt)
        if not res:
            raise BoundaryNotFoundException()
        return res

    async def get_all_boundary_summaries(self) -> List[models.Boundary]:
        """
        Get summary information about all available boundaries
        """
        stmt = select(
            models.Boundary.id, models.Boundary.name, models.Boundary.name_long
        ).order_by(models.Boundary.name_long)
        res = await self.database.fetch_all(stmt)
        if not res:
            return []
        return res

    async def search_boundaries_by_name(self, name: str) -> List[models.Boundary]:
        """
        Search for boundaries by fuzzy matching matching name
        """
        stmt = (
            select(models.Boundary)
            .where(
                func.like(func.lower(models.Boundary.name_long), f"%{name.lower()}%")
            )
            .order_by(models.Boundary.name_long)
        )
        res = await self.database.fetch_all(stmt)
        if not res:
            return []
        return res

    async def search_boundaries_by_coordinates(
        self, latitude: float, longitude: float
    ) -> List[models.Boundary]:
        """
        Spatial search for boundaries intersecting
        """
        stmt = select(models.Boundary).where(
            func.ST_intersects(
                func.ST_SetSRID(func.ST_MakePoint(longitude, latitude), 4326),
                models.Boundary.geometry,
            )
        )
        res = await self.database.fetch_all(stmt)
        if not res:
            return []
        return res
