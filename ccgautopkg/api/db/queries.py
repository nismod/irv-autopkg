"""
DB Queries - Postgres
"""

from fastapi.logger import logger
from databases import Database

from api.config import LOG_LEVEL

logger.setLevel(LOG_LEVEL)

class Queries:
    def __init__(self, database: Database):
        self.database = database
