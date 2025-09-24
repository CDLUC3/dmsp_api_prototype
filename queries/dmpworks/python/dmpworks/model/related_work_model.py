from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel

from dmpworks.model.common import to_camel
from dmpworks.model.work_model import WorkModel


class DoiMatch(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
    }

    found: bool
    score: float
    url: Optional[str] = None


class ContentMatch(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
    }

    score: float
    title_highlight: Optional[str]
    abstract_highlights: List[str]


class ItemMatch(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
    }

    index: int
    score: float
    fields: Optional[List[str]] = None


class RelatedWork(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
    }

    dmp_doi: str
    work: WorkModel
    score: float
    doi_match: DoiMatch
    content_match: ContentMatch
    author_matches: List[ItemMatch] = []
    institution_matches: List[ItemMatch] = []
    funder_matches: List[ItemMatch] = []
    award_matches: List[ItemMatch] = []
