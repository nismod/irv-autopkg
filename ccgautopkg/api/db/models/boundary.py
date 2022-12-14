"""DB Models to Support API"""

from sqlalchemy import Column, Integer, String
from geoalchemy2 import Geometry

from . import Base


class Boundary(Base):
    """Boundaries table"""
    __tablename__ = "boundaries"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    name_long = Column(String, index=True)
    admin_level = Column(String, index=True)
    geometry = Column(Geometry("MULTIPOLYGON", srid=4326))