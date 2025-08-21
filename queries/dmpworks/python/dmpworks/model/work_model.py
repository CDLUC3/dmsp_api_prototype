import datetime
from functools import cached_property
from typing import Optional

import pendulum
from pydantic import BaseModel, field_serializer, field_validator


class WorkModel(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True,
    }

    doi: str
    publication_date: pendulum.Date
    updated_date: pendulum.DateTime
    source: str
    type: str
    title: Optional[str] = None
    abstract: Optional[str] = None
    affiliation_rors: list[str]
    affiliation_names: list[str]
    author_names: list[str]
    author_orcids: list[str]
    award_ids: list[str]
    funder_ids: list[str]
    funder_names: list[str]

    @cached_property
    def funder_ids_set(self) -> frozenset[str]:
        return frozenset(self.funder_ids)

    @field_validator("publication_date", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v).date()
        elif isinstance(v, datetime.date):
            return pendulum.instance(v)
        return v

    @field_validator("updated_date", mode="before")
    @classmethod
    def parse_pendulum_datetime(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v)
        elif isinstance(v, datetime.datetime):
            return pendulum.instance(v)
        return v

    @field_serializer("publication_date")
    def serialize_pendulum_date(self, v: pendulum.Date):
        return v.to_date_string()

    @field_serializer("updated_date")
    def serialize_pendulum_datetime(self, v: pendulum.DateTime):
        return v.to_iso8601_string()
