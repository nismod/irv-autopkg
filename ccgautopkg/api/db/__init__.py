
import databases

from .models import *

from api.config import DBURI_API

database = databases.Database(str(DBURI_API))

from .controller import DBController