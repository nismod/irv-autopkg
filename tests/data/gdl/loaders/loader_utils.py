import json
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

import sys
import os
import inspect

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
grand_dir = os.path.dirname(os.path.dirname(parent_dir))
sys.path.insert(0, parent_dir)
sys.path.insert(0, grand_dir)

from config import get_db_uri_sync, API_POSTGRES_DB


def load_json(fpath: str):
    with open(fpath, "r") as file:
        data = json.load(file)

    return data


def init_db_session():
    db_uri = get_db_uri_sync(API_POSTGRES_DB)
    engine = sa.create_engine(db_uri, pool_pre_ping=True)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return engine, Session
