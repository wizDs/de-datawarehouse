from datetime import datetime
from sqlmodel import Field, SQLModel


class Postalcode(SQLModel, table=True):
    __tablename__ = "postalcode"
    postalcode: int = Field(primary_key=True, index=True, nullable=False)
    name: str
    longitude: float
    latitude: float
    modified_date: datetime
    geo_modified_date: datetime
    geo_version: int
    dagi_id: str


class Municipality(SQLModel, table=True):
    __tablename__ = "municipality"
    municipalitycode: int = Field(primary_key=True, index=True, nullable=False)
    municipalityname: str


class PostalcodeToMunicipality(SQLModel, table=True):
    __tablename__ = "postalcode_to_municipality"
    postalcode: int = Field(
        primary_key=True, foreign_key='postalcode.postalcode', index=True, nullable=False)
    municipalitycode: int = Field(
        primary_key=True, foreign_key='municipality.municipalitycode', index=True, nullable=False)
