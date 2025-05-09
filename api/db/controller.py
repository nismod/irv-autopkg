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
    return (
        session.query(
            models.Boundary.id, models.Boundary.name, models.Boundary.name_long
        )
        .order_by(models.Boundary.name_long)
        .all()
    )


def get_boundary_by_name(name: str, session) -> schemas.Boundary:
    """
    Retrieve detail about a specific named boundary
    """
    boundary = (
        session.query(
            models.Boundary.id,
            models.Boundary.name,
            models.Boundary.name_long,
            models.Boundary.admin_level,
            func.ST_AsGeoJSON(models.Boundary.geometry).label("geometry"),
            func.ST_AsGeoJSON(func.ST_Envelope(models.Boundary.geometry)).label(
                "envelope"
            ),
        )
        .where(models.Boundary.name == name)
        .one()
    )
    if not boundary:
        raise BoundaryNotFoundException()

    return schemas.Boundary(
        id=boundary.id,
        name=boundary.name,
        name_long=boundary.name_long,
        admin_level=boundary.admin_level,
        geometry=json.loads(boundary.geometry),
        envelope=json.loads(boundary.envelope),
    )
