"""
Test Helpers
"""
import os
import sys
import inspect

import sqlalchemy as sa

from api.config import get_db_uri_sync, API_DB_NAME, INTEGRATION_TEST_ENDPOINT
from api import db

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

db_uri = get_db_uri_sync(API_DB_NAME)
# Init DB and Load via SA
engine = sa.create_engine(db_uri, pool_pre_ping=True)

def wipe_db():
    """Wipe all SQLA Tables in the DB"""
    print("Running wipe db...")
    db_uri = get_db_uri_sync(API_DB_NAME)
    # Init DB and Load via SA
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    for tbl in reversed(db.Base.metadata.sorted_tables):
        engine.execute(tbl.delete())
    print("Wipe db has run")

def build_route(postfix_url: str):
    return "{}{}".format(
        INTEGRATION_TEST_ENDPOINT, postfix_url
    )