"""
DB Controller - Postgres
"""

from fastapi.logger import logger

from api.config import LOG_LEVEL
from api.db import database
from api import schemas
from api.db.queries import Queries

logger.setLevel(LOG_LEVEL)

class DBController:
    def __init__(self):
        pass
