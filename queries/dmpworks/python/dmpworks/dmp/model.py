from __future__ import annotations

import dataclasses
import datetime
from typing import Optional

import pendulum
from pydantic import BaseModel, field_serializer, field_validator

from dmpworks.funders.award_id import AwardID


class DMPModel(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True,
    }

    doi: str
    created: Optional[pendulum.Date]
    registered: Optional[pendulum.Date]
    modified: Optional[pendulum.Date]
    title: Optional[str]
    abstract: Optional[str]
    project_start: Optional[pendulum.Date]
    project_end: Optional[pendulum.Date]
    affiliation_rors: list[str]
    affiliation_names: list[str]
    author_names: list[AuthorName]
    author_orcids: list[str]
    funding: list[FundingItem]
    external_data: Optional[ExternalData] = None

    @field_validator("created", "registered", "modified", "project_start", "project_end", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v).date()
        elif isinstance(v, datetime.date):
            return pendulum.instance(v)
        return v

    @field_serializer("created", "registered", "modified", "project_start", "project_end")
    def serialize_pendulum_date(self, v: pendulum.Date):
        return v.to_date_string()


class Funder(BaseModel):
    name: Optional[str]
    id: Optional[str]


class FundingItem(BaseModel):
    funder: Optional[Funder]
    funding_opportunity_id: Optional[str]
    status: Optional[str]
    award_id: Optional[str]


class AuthorName(BaseModel):
    first_initial: Optional[str]
    given_name: Optional[str]
    middle_initials: Optional[str]
    middle_names: Optional[str]
    surname: Optional[str]
    full: Optional[str]


class ExternalData(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    updated: pendulum.DateTime
    awards: list[Award]

    @field_validator("updated", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v)
        elif isinstance(v, datetime.datetime):
            return pendulum.instance(v)
        return v

    @field_serializer("updated")
    def serialize_pendulum_date(self, v: pendulum.DateTime):
        return v.to_iso8601_string()


class Award(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    funder: Optional[Funder]
    award_id: Optional[AwardID]
    funded_dois: list[str]

    @field_validator("award_id", mode="before")
    @classmethod
    def parse_award_id(cls, v):
        if isinstance(v, AwardID):
            return v
        if isinstance(v, dict):
            return AwardID.from_dict(v)
        raise TypeError(f"Expected MyClass or dict, got {type(v)}")

    @field_serializer("award_id")
    def serialize_award_id(self, v: AwardID):
        return v.to_dict()
