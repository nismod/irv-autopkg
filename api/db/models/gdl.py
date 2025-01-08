"""DB Models to Support GDL API"""

from sqlalchemy import Column, String, SmallInteger, Float, ForeignKey
from sqlalchemy.orm import relationship, declared_attr

from geoalchemy2 import Geometry


from . import Base


class GdlRegion(Base):
    """GDL GeoJSON boundaries table"""

    __tablename__ = "gdl_region"

    gdl_code = Column(String, primary_key=True, index=True)
    region_name = Column(String, nullable=False)
    level = Column(String, nullable=False)
    iso_code = Column(String, ForeignKey("iso_country.iso_code"), nullable=False)
    iso_country = relationship("IsoCountry", back_populates="gdl_regions")


class IsoCountry(Base):
    """ISO country codes table"""

    __tablename__ = "iso_country"

    iso_code = Column(String, primary_key=True, index=True)
    country_name = Column(String, nullable=False)
    continent = Column(String, nullable=False)
    gdl_regions = relationship("GdlRegion", back_populates="iso_country")


class GdlBoundary(Base):
    """GDL GeoJSON boundaries table"""

    __abstract__ = True

    @declared_attr
    def gdl_code(cls):
        return Column(
            String,
            ForeignKey("gdl_region.gdl_code"),
            primary_key=True,
            index=True,
        )

    @declared_attr
    def gdl_region(cls):
        return relationship("GdlRegion")

    geometry = Column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)


class GdlSubnational(GdlBoundary):
    __tablename__ = "gdl_subnational"


class GdlNational(GdlBoundary):
    __tablename__ = "gdl_national"


class GdlAnnual(Base):
    __abstract__ = True

    @declared_attr
    def gdl_code(cls):
        return Column(
            String,
            ForeignKey("gdl_region.gdl_code"),
            primary_key=True,
            index=True,
        )

    @declared_attr
    def gdl_region(cls):
        return relationship("GdlRegion")

    year = Column(SmallInteger, primary_key=True, index=True)
    value = Column(Float, nullable=False)


class DevelopmentAnnual(GdlAnnual):
    __tablename__ = "development"


class EducationAnnual(GdlAnnual):
    __tablename__ = "education"


class HealthcareAnnual(GdlAnnual):
    __tablename__ = "healthcare"


class IncomeAnnual(GdlAnnual):
    __tablename__ = "income"
