from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class Institution(BaseModel):
    name: Optional[str]
    ror: Optional[str]


class Author(BaseModel):
    orcid: Optional[str]
    first_initial: Optional[str]
    given_name: Optional[str]
    middle_initials: Optional[str]
    middle_names: Optional[str]
    surname: Optional[str]
    full: Optional[str]


class Funder(BaseModel):
    name: Optional[str]
    ror: Optional[str]


class Source(BaseModel):
    name: Optional[str]
    url: Optional[str]
