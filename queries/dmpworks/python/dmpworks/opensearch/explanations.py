from __future__ import annotations

import dataclasses
import json
from collections import defaultdict
from typing import Optional

from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.work_model import WorkModel

MatchedQueries = dict[str, float] | list[str]


@dataclasses.dataclass(kw_only=True)
class Group:
    name: str
    score: Optional[float] = None
    fields: list[Entity] = dataclasses.field(default_factory=list)
    entities: dict[int, Entity] = dataclasses.field(default_factory=lambda: defaultdict(Entity))

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "score": self.score,
            "fields": [entity.to_dict() for entity in self.fields],
            "entities": {idx: entity.to_dict() for idx, entity in sorted(self.entities.items())},
        }


@dataclasses.dataclass(kw_only=True)
class Match:
    value: Optional[str] = None
    score: Optional[float] = None
    source: Optional[dict] = dataclasses.field(default_factory=dict)
    matched: bool = False

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "score": self.score,
            "source": self.source,
            "matched": self.matched,
        }


@dataclasses.dataclass(kw_only=True)
class Entity(defaultdict[str, Match]):
    score: Optional[float] = None

    def __post_init__(self):
        defaultdict.__init__(self, Match)

    def to_dict(self) -> dict:
        fields = {}
        for key, match in self.items():
            if isinstance(match, Match):
                fields[key] = match.to_dict()
        return {
            "score": self.score,
            **fields,
        }


def parse_matched_queries(matched_queries: MatchedQueries):
    groups = []
    entities = []
    values = []

    # When queries is a list add None scores
    if isinstance(matched_queries, list):
        queries = [(key, None) for key in matched_queries]
    elif isinstance(matched_queries, dict):
        queries = list(matched_queries.items())
    else:
        raise TypeError(f"parse_matched_queries: unknown matched_queries type {type(matched_queries)}")

    for key, score in queries:
        try:
            data = json.loads(key)
        except Exception:
            continue

        group_name = data.get("g")
        level_name = data.get("lvl")
        idx = data.get("i")
        key = data.get("k")
        value = data.get("v")

        if level_name == "group":
            groups.append((group_name, score))
        elif level_name == "entity":
            entities.append((group_name, idx, score))
        elif level_name == "value":
            values.append((group_name, idx, key, value, score))

    return groups, entities, values


def explain_match(
    dmp: DMPModel, work: WorkModel, matched_queries: MatchedQueries, highlights: dict[str, list[str]]
) -> dict[str, Group]:
    # TODO: merge DMP data when we have full DMP schema, as then we can
    # show what did not match

    if matched_queries is None:
        return {}

    explanations = {}
    groups, entities, values = parse_matched_queries(matched_queries)

    # Create groups
    for group_name, score in groups:
        group = Group(name=group_name, score=score)
        if group_name == "funded_dois":
            for idx, award in enumerate(dmp.external_data.awards):
                if work.doi in award.funded_dois_set:
                    funded_doi = Entity(score=score)
                    funded_doi["doi"].value = work.doi
                    funded_doi["doi"].score = score
                    funded_doi["doi"].source = award.award_id.funded_dois_source()
                    funded_doi["doi"].matched = True
                    group.entities[idx] = funded_doi
        elif group_name == "content":
            fields = []
            for field in ("title", "abstract"):
                highlights_list = highlights.get(field, [])
                if len(highlights_list) > 0:
                    content = Entity(score=score)
                    content[field] = Match(
                        value="\n".join(highlights_list),
                        score=score,
                        matched=True,
                    )
                    fields.append(content)
            group.fields = fields
        explanations[group_name] = group

    # Create entities
    for group_name, idx, score in entities:
        explanations[group_name].entities[idx] = Entity(score=score)

    # Create values
    # TODO: authors and affiliations are not working yet, they need to be
    # organised as objects in the DMP
    for group_name, idx, key, value, score in values:
        entity = explanations[group_name].entities[idx]
        entity[key].score = score
        entity[key].value = value
        entity[key].matched = True

    return explanations
