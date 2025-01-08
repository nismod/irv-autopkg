import json
from typing import List
from fastapi import APIRouter, HTTPException
import logging
from config import LOG_LEVEL
from api.routes import (
    GDL_BOUNDARY_FROM_ISO_ROUTE,
    GDL_COUNTRY_META_ROUTE,
    GDL_REGION_META_ROUTE,
    GDL_DATA_ISO_ROUTE,
    GDL_DATA_ALL_ROUTE,
    GDL_NATIONAL_FROM_ISO_ROUTE,
)
from api.helpers import handle_exception

from api.db import database, models
from sqlalchemy.sql import select, func
from api import schemas


router = APIRouter(
    tags=["boundaries"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger("uvicorn.access")
logger.setLevel(LOG_LEVEL)


dataset_key_to_model = {
    "development": models.DevelopmentAnnual,
    "education": models.EducationAnnual,
    "income": models.IncomeAnnual,
    "healthcare": models.HealthcareAnnual,
}


def parse_gdl_annual(results):
    return [
        {
            "gdl_code": result["gdl_code"],
            "region_name": result["region_name"],
            "iso_code": result["iso_code"],
            "year": result["year"],
            "value": result["value"],
        }
        for result in results
    ]


def select_gdl_annual(Model):
    return select(Model, models.GdlRegion.region_name, models.GdlRegion.iso_code).join(
        models.GdlRegion
    )


@router.get(GDL_DATA_ALL_ROUTE, response_model=List[schemas.GdlAnnualData])
async def get_all_annual_data(dataset_key: str):
    """Read all annual data for single dataset"""
    try:
        Model = dataset_key_to_model[dataset_key]
        stmt = select_gdl_annual(Model).order_by(Model.gdl_code, Model.year)
        results = await database.fetch_all(stmt)

        return parse_gdl_annual(results)

    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(GDL_DATA_ISO_ROUTE, response_model=schemas.GdlCountryAnnual)
async def get_annual_for_country(dataset_key: str, iso_code: str):
    """Read all annual data for single country and dataset"""
    try:
        Model = dataset_key_to_model[dataset_key]

        # Get extents for scaling axes
        stmt_full_dataset = select_gdl_annual(Model).order_by(
            Model.gdl_code, Model.year
        )
        results = await database.fetch_all(stmt_full_dataset)
        parsed_full_data = parse_gdl_annual(results)
        years = set()
        for record in parsed_full_data:
            years.add(record["year"])

        year_extents = {}
        for year in years:
            year_extents[year] = {"min": None, "max": None}

        for record in parsed_full_data:
            year = record["year"]
            if (
                year_extents[year]["min"] is None
                or year_extents[year]["min"] > record["value"]
            ):
                year_extents[year] = {
                    "min": record["value"],
                    "max": year_extents[year]["max"],
                }

            if (
                year_extents[year]["max"] is None
                or year_extents[year]["max"] < record["value"]
            ):
                year_extents[year] = {
                    "min": year_extents[year]["max"],
                    "max": record["value"],
                }

        stmt = (
            select_gdl_annual(Model)
            .where(models.GdlRegion.iso_code == iso_code)
            .order_by(Model.gdl_code, Model.year)
        )
        results = await database.fetch_all(stmt)

        return {"data": parse_gdl_annual(results), "extents": year_extents}

    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(GDL_COUNTRY_META_ROUTE, response_model=List[schemas.GdlCountryMeta])
async def get_all_countries_meta():
    """Read all countries metadata"""
    try:
        stmt = select(models.IsoCountry)
        results = await database.fetch_all(stmt)

        data = [
            {
                "iso_code": result.iso_code,
                "country_name": result.country_name,
                "continent": result.continent,
            }
            for result in results
        ]

        return data

    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(GDL_REGION_META_ROUTE, response_model=List[schemas.GdlRegionMeta])
async def get_all_regions_meta():
    """Read all region metadata"""
    try:
        stmt = select(models.GdlRegion)
        results = await database.fetch_all(stmt)

        data = [
            {
                "gdl_code": result.gdl_code,
                "region_name": result.region_name,
                "level": result.level,
                "iso_code": result.iso_code,
            }
            for result in results
        ]

        return data

    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(GDL_NATIONAL_FROM_ISO_ROUTE, response_model=schemas.GdlNationalGeo)
async def get_national_for_iso(iso_code: str):
    """Read all national GeoJSON tied to a single country iso_code"""
    try:
        stmt = (
            select(
                func.ST_AsGeoJSON(models.GdlNational.geometry),
                func.ST_AsGeoJSON(func.ST_Envelope(models.GdlNational.geometry)),
                models.GdlNational.gdl_code,
                models.GdlRegion.region_name,
                models.GdlRegion.level,
                models.GdlRegion.iso_code,
                models.IsoCountry.country_name,
            )
            .join(
                models.GdlRegion,
                models.GdlRegion.gdl_code == models.GdlNational.gdl_code,
            )
            .join(
                models.IsoCountry,
                models.GdlRegion.iso_code == models.IsoCountry.iso_code,
            )
            .where(models.GdlRegion.iso_code == iso_code)
        )
        result = await database.fetch_one(stmt)

        # TODO

        data = {
            "boundary": {
                "type": "Feature",
                "geometry": json.loads(result.ST_AsGeoJSON_1),
            },
            "envelope": json.loads(result.ST_AsGeoJSON_2),
            "properties": {
                "gdl_code": result.gdl_code,
                "region_name": result.region_name,
                "level": result.level,
                "iso_code": result.iso_code,
                "country_name": result.country_name,
            },
        }

        return data

    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)


@router.get(GDL_BOUNDARY_FROM_ISO_ROUTE, response_model=List[schemas.GdlSubnationalGeo])
async def get_all_boundaries_for_iso(iso_code: str):
    """Read all GeoJSON tied to a single country iso_code"""
    try:
        stmt = (
            select(
                func.ST_AsGeoJSON(models.GdlSubnational.geometry),
                models.GdlSubnational.gdl_code,
                models.GdlRegion.region_name,
                models.GdlRegion.level,
                models.GdlRegion.iso_code,
            )
            .join(models.GdlRegion)
            .where(models.GdlRegion.iso_code == iso_code)
        )
        results = await database.fetch_all(stmt)

        data = [
            {
                "type": "Feature",
                "geometry": json.loads(result.ST_AsGeoJSON_1),
                "properties": {
                    "gdl_code": result.gdl_code,
                    "region_name": result.region_name,
                    "level": result.level,
                    "iso_code": result.iso_code,
                },
            }
            for result in results
        ]

        return data

    except Exception as err:
        handle_exception(logger, err)
        raise HTTPException(status_code=500)
