import databases

from config import DBURI_API

from .models import boundary

database = databases.Database(DBURI_API)

__all__ = ["boundary", "database"]
