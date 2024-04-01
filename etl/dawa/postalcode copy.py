# Module Imports
import os
from datetime import datetime, timedelta
from operator import itemgetter
from typing import NamedTuple
import dotenv
import polars as pl
from pydantic import BaseModel
from sqlmodel import select, SQLModel, create_engine, Session
import requests
import etl.dawa.schemas as schemas

TIMEOUT = timedelta(seconds=60)

dotenv.load_dotenv()

# Connect to MariaDB Platform
engine = create_engine(url=os.getenv('CONNECTION_STRING'),
                       echo=True)

with Session(engine) as session:
    statement = select(schemas.Municipality)
    result = session.exec(statement).all()

print(result)
