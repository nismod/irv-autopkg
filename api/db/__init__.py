
import databases

from config import DBURI_API

from .models import *

database = databases.Database(str(DBURI_API))

from .controller import DBController