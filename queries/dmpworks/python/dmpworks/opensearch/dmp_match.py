import dataclasses
import datetime
from typing import Any, Optional

import pendulum
from opensearchpy import OpenSearch
from pydantic import BaseModel, field_serializer, field_validator
from tqdm import tqdm
from fold_to_ascii import fold
from dmpworks.dmp.model import DMPModel
from dmpworks.opensearch.utils import (
    make_opensearch_client,
    OpenSearchClientConfig,
    yield_dmps,
)
from dmpworks.utils import timed


MatchedQueries = dict[str, float]


@dataclasses.dataclass(kw_only=True)
class DMPMatchModel:
    doi: str
    project_start: pendulum.Date
    project_end: pendulum.Date
    title: Optional[str]
    abstract: Optional[str]
    affiliation_rors: list[str] = dataclasses.field(default_factory=list)
    affiliation_names: list[str] = dataclasses.field(default_factory=list)
    author_names: list[str] = dataclasses.field(default_factory=list)
    author_orcids: list[str] = dataclasses.field(default_factory=list)
    award_ids: list[str] = dataclasses.field(default_factory=list)
    funder_ids: list[str] = dataclasses.field(default_factory=list)
    funder_names: list[str] = dataclasses.field(default_factory=list)
    funded_dois: list[str] = dataclasses.field(default_factory=list)


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


def to_dmp_match_model(model: DMPModel) -> DMPMatchModel:
    funder_ids = set()
    funder_names = set()
    funded_dois = set()
    award_ids = set()
    for award in model.external_data.awards:
        funder_ids.add(award.funder.id)
        funder_names.add(award.funder.name)
        for doi in award.funded_dois:
            funded_dois.add(doi)
        for award_id in award.award_id.generate_variants():
            award_ids.add(award_id)

    author_names = []
    for name in model.author_names:
        if name.surname is not None:
            author_names.append(name.surname)
        else:
            author_names.append(name.full)

    return DMPMatchModel(
        doi=model.doi,
        project_start=model.project_start,
        project_end=model.project_end,
        title=model.title,
        abstract=model.abstract,
        affiliation_rors=model.affiliation_rors,
        affiliation_names=model.affiliation_names,
        author_names=author_names,
        author_orcids=model.author_orcids,
        award_ids=list(award_ids),
        funder_ids=list(funder_ids),
        funder_names=list(funder_names),
        funded_dois=list(funded_dois),
    )


def match_ids(dmp_ids: list[str], work_ids: list[str]) -> list[str]:
    values = []
    work_norm = {work_id.lower() for work_id in work_ids}
    for dmp_id in dmp_ids:
        if dmp_id.lower() in work_norm:
            values.append(dmp_id)
    return values


def match_text(dmp_items: list[str], work_items: list[str]) -> list[str]:
    values = []
    work_norm = fold(" ".join([text for text in work_items]).lower())
    for dmp_item in dmp_items:
        if fold(dmp_item.lower()) in work_norm:
            values.append(dmp_item)
    return values


@dataclasses.dataclass(kw_only=True)
class Explanation:
    name: str
    score: float
    values: list[Any] = dataclasses.field(default_factory=list)


def explain_match(
    dmp: DMPMatchModel,
    work: WorkModel,
    max_score: float,
    matched_queries: MatchedQueries,
) -> list[Explanation]:
    # TODO: when we have the structured DMP, we can return more structured
    # information here
    explanations = []

    for name, score in matched_queries.items():
        # Funded DOIs
        if name == "funded_dois":
            explanations.append(
                Explanation(
                    name=name,
                    score=score,
                    values=[dmp.doi],
                    # TODO: which specific award did we find it from and where is the link where you can view the
                    # TODO: publications?
                    # description="Work DOI was found at https://xxx associated with Award XXX",
                )
            )

        # Award IDs
        if name == "awards":
            explanations.append(
                Explanation(
                    name=name,
                    score=score,
                    values=[dmp.award_ids],
                )
            )

        # Authors
        # ORCiD ID, surname
        if name == "authors":
            values = match_ids(dmp.author_orcids, work.author_orcids)
            values.extend(match_ids(dmp.author_names, work.author_names))
            explanations.append(
                Explanation(
                    name=name,
                    score=score,
                    values=values,
                )
            )

        # Affiliations
        # ROR and names
        if name == "affiliations":
            values = match_ids(dmp.affiliation_rors, work.affiliation_rors)
            values.extend(match_ids(dmp.affiliation_names, work.affiliation_names))
            explanations.append(
                Explanation(
                    name=name,
                    score=score,
                    values=values,
                )
            )

        # Funders
        # Name and ROR
        if name == "funders":
            values = match_ids(dmp.funder_ids, work.funder_ids)
            values.extend(match_ids(dmp.funder_names, work.funder_names))
            explanations.append(
                Explanation(
                    name=name,
                    score=score,
                    values=values,
                )
            )

        # title and abstract
        if name == "content":
            explanations.append(
                Explanation(
                    name=name,
                    score=score,
                )
            )

    return explanations


@timed
def dmp_match_search(
    dmp_index_name: str,
    works_index_name: str,
    client_config: OpenSearchClientConfig,
    scroll_time: str = "60m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    explain: bool = False,
):
    client = make_opensearch_client(client_config)
    query = {"query": {"match_all": {}}}
    all_matches = []
    with tqdm(
        total=0,
        desc="Find DMP matches with OpenSearch",
        unit="doc",
    ) as pbar:
        with yield_dmps(
            client,
            dmp_index_name,
            query,
            page_size=batch_size,
            scroll_time=scroll_time,
        ) as results:
            pbar.total = results.total_dmps

            for dmp in results.dmps:
                matches = opensearch_dmp_match_search(
                    client,
                    works_index_name,
                    dmp,
                    max_results=max_results,
                    project_end_buffer_years=project_end_buffer_years,
                )
                all_matches.extend(matches)
                pbar.update(1)
    all_matches.sort(key=lambda x: x[2], reverse=True)
    a = 1


def opensearch_dmp_match_search(
    client: OpenSearch,
    index_name: str,
    dmp: DMPModel,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
) -> list[tuple[str, str, float, WorkModel, list[Explanation]]]:
    # Execute search
    dmp_match_model = to_dmp_match_model(dmp)
    query = build_query(dmp_match_model, max_results, project_end_buffer_years)
    response = client.search(
        body=query,
        index=index_name,
        track_total_hits=True,
        include_named_queries_score=True,
    )

    # Process results
    results = []
    total_hits = response.get("hits", {}).get("total", {}).get("value", 0)
    max_score = response.get("hits", {}).get("max_score")
    hits = response["hits"]["hits"]
    for hit in hits:
        work_doi: str = hit["_id"]
        score: float = hit["_score"]
        work = WorkModel.model_validate(hit["_source"])
        matched_queries = hit["matched_queries"]
        explanation = explain_match(dmp_match_model, work, max_score, matched_queries)
        results.append((dmp.doi, work_doi, score, work, explanation))

    return results


def build_query(dmp: DMPMatchModel, max_results: int, project_end_buffer_years: int) -> dict:
    # Funded DOIs
    should = [
        {
            "terms": {
                "_name": "funded_dois",
                "doi": dmp.funded_dois,
                "boost": 25,
            },
        }
    ]

    # Award IDs
    weight = 20
    num_awards = len(dmp.award_ids)
    should.append(
        {
            "function_score": {
                "_name": "awards",
                "query": {
                    "terms": {
                        "award_ids": dmp.award_ids,
                    }
                },
                "functions": [
                    {
                        "filter": {"match_all": {}},
                        "weight": 1.0 * weight,
                        "_name": "award_ids",
                    }
                ],
                "score_mode": "sum",
                "boost_mode": "replace",
                "max_boost": num_awards * weight,
            }
        },
    )

    # Authors
    # Combines author_orcids and author surnames into a single feature
    weight = 2.0
    num_authors = max(len(dmp.affiliation_rors), len(dmp.affiliation_names))
    should.append(
        {
            "function_score": {
                "_name": "authors",
                "query": {
                    "bool": {
                        "should": [
                            {"terms": {"author_orcids": dmp.author_orcids}},
                            *[
                                {
                                    "match_phrase": {
                                        "author_names": {
                                            "query": name,
                                        }
                                    }
                                }
                                for name in dmp.author_names
                            ],
                        ],
                        "minimum_should_match": 1,
                    },
                },
                "functions": [
                    {
                        "filter": {"match_all": {}},
                        "weight": 1.0 * weight,
                        "_name": "author_score",
                    },
                ],
                "score_mode": "sum",
                "boost_mode": "replace",
                "max_boost": num_authors * weight,
            }
        }
    )

    # Affiliations
    # Combines both affiliation_rors and affiliation_names into a single feature
    weight = 1.0
    num_affiliations = max(len(dmp.affiliation_rors), len(dmp.affiliation_names))
    should.append(
        {
            "function_score": {
                "_name": "affiliations",
                "query": {
                    "bool": {
                        "should": [
                            {
                                "terms": {
                                    "affiliation_rors": dmp.affiliation_rors,
                                }
                            },
                            *[
                                {
                                    "match_phrase": {
                                        "affiliation_names": {
                                            "query": name,
                                            "slop": 3,
                                        }
                                    },
                                }
                                for name in dmp.affiliation_names
                            ],
                        ],
                        "minimum_should_match": 1,
                    },
                },
                "functions": [
                    {
                        "filter": {"match_all": {}},
                        "weight": 1.0 * weight,
                        "_name": "affiliation_score",
                    },
                ],
                "score_mode": "sum",
                "boost_mode": "replace",
                "max_boost": num_affiliations * weight,
            }
        }
    )

    # Funders
    # Combines both funder_ids and funder_names into a single feature
    weight = 1.0
    num_funders = max(len(dmp.funder_ids), len(dmp.funder_names))
    should.append(
        {
            "function_score": {
                "_name": "funders",
                "query": {
                    "bool": {
                        "should": [
                            {
                                "terms": {
                                    "funder_ids": dmp.funder_ids,
                                }
                            },
                            *[
                                {
                                    "match_phrase": {
                                        "funder_names": {
                                            "query": name,
                                            "slop": 3,
                                        }
                                    }
                                }
                                for name in dmp.funder_names
                            ],
                        ],
                        "minimum_should_match": 1,
                    },
                },
                "functions": [
                    {
                        "filter": {"match_all": {}},
                        "weight": 1.0 * weight,
                        "_name": "funder_score",
                    },
                ],
                "score_mode": "sum",
                "boost_mode": "replace",
                "max_boost": num_funders * weight,
            }
        }
    )

    # Title and abstract
    content: str = " ".join([text for text in [dmp.title, dmp.abstract] if text is not None and text != ""])
    should.append(
        {
            "more_like_this": {
                "_name": "content",
                "fields": ["title", "abstract"],
                "like": content,
                "min_term_freq": 1,
            }
        }
    )

    # Final query and filter based on date range
    gte = dmp.project_start.format("YYYY-MM-DD")
    lte = dmp.project_end.add(years=project_end_buffer_years).format("YYYY-MM-DD")

    return {
        "size": max_results,
        "query": {
            "bool": {
                "should": should,
                "filter": [
                    {
                        "range": {
                            "publication_date": {
                                "gte": gte,
                                "lte": lte,
                            },
                        }
                    },
                ],
                "minimum_should_match": 1,
            },
        },
    }


if __name__ == "__main__":
    dmp_match_search(
        "dmps-index",
        "works-index-demo",
        OpenSearchClientConfig(),
        max_results=5,
    )
