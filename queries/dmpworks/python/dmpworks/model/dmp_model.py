from __future__ import annotations

import datetime
from functools import cached_property
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

    @cached_property
    def author_surnames(self) -> list[str]:
        surnames = set()
        for name in self.author_names:
            if name.surname is not None:
                surnames.add(name.surname)
            else:
                surnames.add(name.full)
        return list(surnames)

    @cached_property
    def funder_ids(self) -> list[str]:
        funder_ids = set()
        for fund in self.funding:
            if fund.funder.id is not None:
                funder_ids.add(fund.funder.id)
        return list(funder_ids)

    @cached_property
    def funder_names(self) -> list[str]:
        funder_names = set()
        for fund in self.funding:
            if fund.funder.name is not None:
                funder_names.add(fund.funder.name)
        return list(funder_names)

    @cached_property
    def funded_dois(self) -> list[str]:
        funded_dois = set()
        for award in self.external_data.awards:
            for doi in award.funded_dois:
                funded_dois.add(doi)
        return list(funded_dois)

    @cached_property
    def award_ids(self) -> list[str]:
        award_ids = set()
        for award in self.external_data.awards:
            # Add variants for root award
            for award_id in award.award_id.generate_variants():
                award_ids.add(award_id)

            # Add award IDs for related awards
            for related_award in award.award_id.related_awards:
                for award_id in related_award.generate_variants():
                    award_ids.add(award_id)

        return list(award_ids)

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
    award_url: Optional[str] = None

    @cached_property
    def funded_dois_set(self) -> frozenset[str]:
        return frozenset(self.funded_dois)

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
