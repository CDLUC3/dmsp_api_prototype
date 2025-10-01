import datetime
import hashlib
from functools import cached_property
from typing import Optional
import json
import pendulum
from pydantic import BaseModel, computed_field, field_serializer, field_validator

from dmpworks.model.common import Author, Award, Funder, Institution, Source, to_camel


class WorkModel(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    doi: str
    title: Optional[str] = None
    abstract: Optional[str] = None
    type: str
    publication_date: pendulum.Date
    updated_date: pendulum.DateTime
    publication_venue: Optional[str] = None
    institutions: list[Institution]
    authors: list[Author]
    funders: list[Funder]
    awards: list[Award]
    source: Source

    @computed_field
    def hash(self) -> str:
        """Generate a stable MD5 Hash of the work based on its content

        Exclude doi and updated_date. Fields that we are not using could
        trigger a change in updated_date.
        """
        data = self.model_dump(
            exclude={"doi", "updated_date", "hash"},
            by_alias=True,
            mode="json",
        )

        # Maintain stable key order: sort_keys=True
        # No whitespace: separators=(",", ":")
        payload = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

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
