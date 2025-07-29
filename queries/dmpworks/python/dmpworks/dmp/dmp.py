from __future__ import annotations

from typing import Optional

import pendulum
from pydantic import BaseModel, field_serializer, field_validator


class DMP(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True,
    }

    dmp_id: str
    created: pendulum.Date
    registered: pendulum.Date
    modified: pendulum.Date
    title: str
    description: str
    project_start: pendulum.Date
    project_end: pendulum.Date
    affiliation_ids: list[str]
    affiliations: list[str]
    people: list[str]
    people_ids: list[str]
    funding: list[FundingItem]
    repo_ids: list[str]
    repos: list[str]
    visibility: str
    featured: Optional[str]

    @field_validator("created", "registered", "modified", "project_start", "project_end", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v).date()
        return v

    @field_serializer("created", "registered", "modified", "project_start", "project_end")
    def serialize_pendulum_date(self, v: pendulum.Date):
        return v.to_date_string()


class Funder(BaseModel):
    name: str
    id: str


class FundingItem(BaseModel):
    funder: Funder
    funding_opportunity_id: Optional[str]
    status: str
    grant_id: Optional[str]
