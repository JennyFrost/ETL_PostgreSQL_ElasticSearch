import uuid
from datetime import datetime

from pydantic import BaseModel, constr, Field

class Person(BaseModel):
    person_id: uuid.UUID
    person_name: str
    person_role: str

class Filmwork(BaseModel):
    fw_id: uuid.UUID
    title: str
    description: str
    rating: float = Field(default_factory=0.0)
    type: str
    persons: list[Person]
    genres: list[str]


