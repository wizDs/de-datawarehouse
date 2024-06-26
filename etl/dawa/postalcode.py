# Module Imports
import os
from datetime import datetime, timedelta
from operator import itemgetter
from typing import NamedTuple
import dotenv
import polars as pl
from pydantic import BaseModel
from sqlmodel import Field, SQLModel, create_engine, Session
import requests
import etl.dawa.schemas as schemas

TIMEOUT = timedelta(seconds=60)
DATABASE = 'dawa'


class BBOX(NamedTuple):
    longitude_start: float
    latitude_start: float
    longitude_end: float
    latitude_end: float


class Coordinates(NamedTuple):
    longitude: float
    latitude: float


class Municipality(BaseModel):
    href: str
    kode: str
    navn: str


class Postalcode(BaseModel):
    nr: int = Field(primary_key=True, index=True, nullable=False)
    navn: str
    bbox: BBOX
    visueltcenter: Coordinates
    kommuner: list[Municipality]
    ændret: datetime
    geo_ændret: datetime
    geo_version: int
    dagi_id: str


dotenv.load_dotenv()

# Connect to MariaDB Platform
engine = create_engine(url=os.getenv('CONNECTION_STRING') + '/' + DATABASE,
                       echo=True)
SQLModel.metadata.create_all(engine)

postalcodes = requests.get(
    url='https://api.dataforsyningen.dk/postnumre', timeout=TIMEOUT.seconds).json()
postalcodes = list(map(Postalcode.model_validate, postalcodes))
postalcodes_df = pl.DataFrame(postalcodes)
postalcodes_table = (postalcodes_df
                     .with_columns(longitude=pl.col('visueltcenter').map_elements(itemgetter(0),
                                                                                  return_dtype=float),
                                   latitude=pl.col('visueltcenter').map_elements(itemgetter(1),
                                                                                 return_dtype=float),
                                   longitude_start=pl.col('bbox').map_elements(itemgetter(0),
                                                                               return_dtype=float),
                                   longitude_end=pl.col('bbox').map_elements(itemgetter(2),
                                                                             return_dtype=float),
                                   latitude_start=pl.col('bbox').map_elements(itemgetter(1),
                                                                              return_dtype=float),
                                   latitude_end=pl.col('bbox').map_elements(itemgetter(3),
                                                                            return_dtype=float),)
                     .drop('bbox', 'visueltcenter', 'kommuner')
                     .rename(mapping={'nr': 'postalcode',
                                      'navn': 'name',
                                      'longitude': 'longitude',
                                      'latitude': 'latitude',
                                      'ændret': 'modified_date',
                                      'geo_ændret': 'geo_modified_date',
                                      'geo_version': 'geo_version',
                                      'dagi_id': 'dagi_id'}))

postalcodes_to_municipality_table = (postalcodes_df
                                     .select('nr', 'kommuner')
                                     .explode('kommuner')
                                     .with_columns(municipalitycode=pl.col('kommuner').map_elements(itemgetter('kode'), return_dtype=str),
                                                   municipalityname=pl.col('kommuner').map_elements(itemgetter('navn'), return_dtype=str))
                                     .rename({'nr': 'postalcode'})
                                     .drop('kommuner'))

municipality_table = postalcodes_to_municipality_table.select(
    'municipalitycode', 'municipalityname').unique()

with Session(engine) as session:
    for row in postalcodes_table.iter_rows(named=True):
        session.add(schemas.Postalcode.model_validate(row))

    for row in postalcodes_to_municipality_table.iter_rows(named=True):
        session.add(schemas.PostalcodeToMunicipality.model_validate(row))

    for row in municipality_table.iter_rows(named=True):
        session.add(schemas.Municipality.model_validate(row))

    session.commit()
