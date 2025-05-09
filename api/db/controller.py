"""
DB Controller - Postgres
"""

import json
from typing import List
from fastapi.logger import logger
from sqlalchemy.sql import select, func

from config import LOG_LEVEL
from api import schemas
from api.db import models
from api.exceptions import BoundaryNotFoundException

logger.setLevel(LOG_LEVEL)


def get_all_boundary_summaries(session) -> List[schemas.BoundarySummary]:
    """
    Retrieve summary info about all boundaries
    """
    stmt = select(
        models.Boundary.id, models.Boundary.name, models.Boundary.name_long
    ).order_by(models.Boundary.name_long)
    res = session.fetch_all(stmt)
    if not res:
        boundaries = []
    else:
        boundaries = res
    return [schemas.BoundarySummary.from_orm(boundary) for boundary in boundaries]


def get_boundary_by_name(name: str, session) -> schemas.Boundary:
    """
    Retrieve detail about a specific named boundary
    """
    stmt = select(
        models.Boundary,
        func.ST_AsGeoJSON(models.Boundary.geometry),
        func.ST_AsGeoJSON(func.ST_Envelope(models.Boundary.geometry)),
    ).where(models.Boundary.name == name)
    boundary = session.fetch_one(stmt)
    if not res:
        raise BoundaryNotFoundException()

    return schemas.Boundary(
        id=boundary.id,
        name=boundary.name,
        name_long=boundary.name_long,
        admin_level=boundary.admin_level,
        geometry=json.loads(boundary.ST_AsGeoJSON_1),
        envelope=json.loads(boundary.ST_AsGeoJSON_2),
    )


def search_boundaries_by_coordinates(
    latitude: float, longitude: float, session
) -> List[schemas.Boundary]:
    """
    Get summary information about boundaries intersecting a specific coordinate
    """
    stmt = select(models.Boundary).where(
        func.ST_intersects(
            func.ST_SetSRID(func.ST_MakePoint(longitude, latitude), 4326),
            models.Boundary.geometry,
        )
    )
    res = session.fetch_all(stmt)
    if not res:
        boundaries = []
    else:
        boundaries = res
    return [schemas.BoundarySummary.from_orm(boundary) for boundary in boundaries]


def search_boundaries_by_name(name: str, session) -> List[schemas.Boundary]:
    """
    Get summary information about boundaries with a name similar to the given
    """
    stmt = (
        select(models.Boundary)
        .where(func.like(func.lower(models.Boundary.name_long), f"%{name.lower()}%"))
        .order_by(models.Boundary.name_long)
    )
    res = session.fetch_all(stmt)
    if not res:
        boundaries = []
    else:
        boundaries = res
    return [schemas.BoundarySummary.from_orm(boundary) for boundary in boundaries]
