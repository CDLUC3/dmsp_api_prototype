from __future__ import annotations

import dataclasses
from typing import Any, Optional

from fold_to_ascii import fold
from whoosh.fields import Schema, TEXT
from whoosh.filedb.filestore import RamStorage
from whoosh.index import FileIndex
from whoosh.qparser import QueryParser

from dmpworks.funders.award_id import FundedDOIsSource
from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.work_model import WorkModel

MatchedQueries = dict[str, float]


@dataclasses.dataclass(kw_only=True)
class Explanation:
    name: str
    score: float
    fields: list[Any] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(kw_only=True)
class Match:
    value: Optional[str] = None
    matched: bool = False


@dataclasses.dataclass(kw_only=True)
class FundedDOIMatch:
    doi: Match = dataclasses.field(default_factory=Match)
    source: FundedDOIsSource


@dataclasses.dataclass(kw_only=True)
class AwardMatch:
    award_id: Match = dataclasses.field(default_factory=Match)


@dataclasses.dataclass(kw_only=True)
class AuthorMatch:
    name: Match = dataclasses.field(default_factory=Match)
    orcid_id: Match = dataclasses.field(default_factory=Match)


@dataclasses.dataclass(kw_only=True)
class AffiliationMatch:
    name: Match = dataclasses.field(default_factory=Match)
    id: Match = dataclasses.field(default_factory=Match)


@dataclasses.dataclass(kw_only=True)
class FunderMatch:
    name: Match = dataclasses.field(default_factory=Match)
    id: Match = dataclasses.field(default_factory=Match)


@dataclasses.dataclass(kw_only=True)
class FunderMatch:
    name: Match = dataclasses.field(default_factory=Match)
    id: Match = dataclasses.field(default_factory=Match)


@dataclasses.dataclass(kw_only=True)
class ContentMatch:
    title: Match = dataclasses.field(default_factory=Match)
    abstract: Match = dataclasses.field(default_factory=Match)


def explain_match(
    dmp: DMPModel, work: WorkModel, matched_queries: MatchedQueries, highlights: dict[str, list[str]]
) -> list[Explanation]:
    explanations = []

    for name, score in matched_queries.items():
        # Funded DOIs
        if name == "funded_dois":
            fields = []
            for award in dmp.external_data.awards:
                if work.doi in award.funded_dois_set:
                    fields.append(
                        FundedDOIMatch(
                            doi=Match(value=work.doi, matched=True),
                            source=award.award_id.funded_dois_source(),
                        )
                    )
            explanations.append(Explanation(name=name, score=score, fields=fields))

        # Award IDs
        if name == "awards":
            fields = [
                AwardMatch(award_id=Match(value=award_id, matched=True))
                for award_id in match_ids(dmp.award_ids, work.award_ids)
            ]
            explanations.append(Explanation(name=name, score=score, fields=fields))

        # Authors
        # ORCiD ID, surname
        # TODO: combine into a single explanation for each author
        # TODO: include the full author name and also the surname that was used
        if name == "authors":
            fields = [
                AuthorMatch(orcid_id=Match(value=orcid_id, matched=True))
                for orcid_id in match_ids(dmp.author_orcids, work.author_orcids)
            ]
            fields.extend(
                [
                    AuthorMatch(name=Match(value=surname, matched=True))
                    for surname in match_phrase(dmp.author_surnames, work.author_names)
                ]
            )
            explanations.append(Explanation(name=name, score=score, fields=fields))

        # Affiliations
        # ROR and names
        # TODO: combine into a single explanation for each affiliation
        if name == "affiliations":
            fields = [
                AffiliationMatch(id=Match(value=ror_id, matched=True))
                for ror_id in match_ids(dmp.affiliation_rors, work.affiliation_rors)
            ]
            fields.extend(
                [
                    AffiliationMatch(name=Match(value=affiliation_name, matched=True))
                    for affiliation_name in match_phrase(dmp.affiliation_names, work.affiliation_names, slop=3)
                ]
            )
            explanations.append(Explanation(name=name, score=score, fields=fields))

        # Funders
        # Name and ROR
        if name == "funders":
            fields = []
            ix = create_in_memory_index(work.funder_names)
            funder_ids = normalize_ids(work.funder_ids)
            with ix.searcher() as searcher:
                parser = QueryParser("content", ix.schema)
                for fund in dmp.funding:
                    funder = fund.funder
                    match = FunderMatch()
                    match.name.matched = phrase_in_searcher(searcher, parser, normalize_phrase(funder.name), slop=3)
                    match.id.matched = fund.funder.id in funder_ids

                    if match.name.matched or match.id.matched:
                        match.name.value = funder.name
                        match.id.value = funder.id
                        fields.append(match)

            explanations.append(Explanation(name=name, score=score, fields=fields))

        # title and abstract
        if name == "content":
            content_match = ContentMatch(title=Match(), abstract=Match())
            title_highlights = highlights.get("title", [])
            content_match.title.matched = len(title_highlights) > 0
            abstract_highlights = highlights.get("abstract", [])
            content_match.abstract.matched = len(abstract_highlights) > 0
            if content_match.title.matched:
                content_match.title.value = "\n".join(title_highlights)
            if content_match.abstract.matched:
                content_match.abstract.value = "\n".join(abstract_highlights)
            explanations.append(Explanation(name=name, score=score, fields=[content_match]))

    return explanations


def match_ids(ids: list[str], work_ids: list[str]) -> list[str]:
    values = []
    work_ids_norm = normalize_ids(work_ids)
    for id in ids:
        if normalize_id(id) in work_ids_norm:
            values.append(id)
    return values


def normalize_id(text: str) -> str:
    return text.lower()


def normalize_ids(ids: list[str]) -> set[str]:
    return {normalize_id(i) for i in ids}


def normalize_text(text: str) -> str:
    return fold(text.lower())


def match_phrase(search_phrases: list[str], documents: list[str], slop: int = 0) -> list[str]:
    if not search_phrases or not documents:
        return []

    ix = create_in_memory_index(documents)

    # Find matches
    matched = []
    with ix.searcher() as searcher:
        parser = QueryParser("content", ix.schema)
        for phrase in search_phrases:
            if phrase_in_searcher(searcher, parser, phrase, slop):
                matched.append(phrase)
    return matched


def create_in_memory_index(documents: list[str]) -> FileIndex:
    # Create in memory index
    schema = Schema(content=TEXT(stored=True))
    storage = RamStorage()
    ix = storage.create_index(schema)

    # Index documents
    writer = ix.writer()
    for doc in documents:
        writer.add_document(content=normalize_phrase(doc))
    writer.commit()

    return ix


def normalize_phrase(phrase: str) -> str:
    return fold(phrase)


def phrase_in_searcher(searcher, parser: QueryParser, phrase: str, slop: int = 0) -> bool:
    query_string = f'"{normalize_phrase(phrase)}"~{slop}'
    query = parser.parse(query_string)
    results = searcher.search(query, limit=1)
    return bool(results)
