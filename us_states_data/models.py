from sqlmodel import SQLModel, Field
from typing import Optional


class State(SQLModel, table=True):
    __tablename__ = "us_states"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    abbreviation: str
    capital: str
    population: int
