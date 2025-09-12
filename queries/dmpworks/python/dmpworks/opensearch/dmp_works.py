import dataclasses
import json
import logging
import pathlib
from typing import Callable, Optional

import jsonlines
from opensearchpy import OpenSearch
from tqdm import tqdm

from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.work_model import WorkModel
from dmpworks.opensearch.explanations import explain_match, Group
from dmpworks.opensearch.utils import make_opensearch_client, OpenSearchClientConfig, yield_dmps
from dmpworks.utils import timed

log = logging.getLogger(__name__)


@timed
def dmp_works_search(
    dmp_index_name: str,
    works_index_name: str,
    out_file: pathlib.Path,
    client_config: OpenSearchClientConfig,
    scroll_time: str = "60m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    parallel_search: bool = True,
    include_named_queries_score: bool = False,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
):
    client = make_opensearch_client(client_config)
    query = {"query": {"match_all": {}}}

    if parallel_search and include_named_queries_score:
        log.warning("Unable to use include_named_queries_score with msearch, query scores will not be returned.")

    def write_works(works: list[DMPWorksSearchResult], count: int):
        for work in works:
            writer.write(work.to_dict())
        pbar.update(count)

    with tqdm(total=0, desc="Find DMP work matches with OpenSearch", unit="doc") as pbar:
        with yield_dmps(
            client,
            dmp_index_name,
            query,
            page_size=batch_size,
            scroll_time=scroll_time,
        ) as results:
            pbar.total = results.total_dmps

            with jsonlines.open(out_file, mode='w') as writer:
                batch = []
                for dmp in results.dmps:
                    if not parallel_search or include_named_queries_score:
                        works = search_dmp_works(
                            client,
                            works_index_name,
                            dmp,
                            max_results=max_results,
                            project_end_buffer_years=project_end_buffer_years,
                            include_named_queries_score=include_named_queries_score,
                        )
                        write_works(works, 1)
                    else:
                        batch.append(dmp)
                        if len(batch) >= batch_size:
                            works = msearch_dmp_works(
                                client,
                                works_index_name,
                                batch,
                                max_results=max_results,
                                project_end_buffer_years=project_end_buffer_years,
                                max_concurrent_searches=max_concurrent_searches,
                                max_concurrent_shard_requests=max_concurrent_shard_requests,
                            )
                            write_works(works, len(batch))
                            batch = []

                if parallel_search and batch:
                    works = msearch_dmp_works(
                        client,
                        works_index_name,
                        batch,
                        max_results=max_results,
                        project_end_buffer_years=project_end_buffer_years,
                        max_concurrent_searches=max_concurrent_searches,
                        max_concurrent_shard_requests=max_concurrent_shard_requests,
                    )
                    write_works(works, len(batch))


@dataclasses.dataclass
class DMPWorksSearchResult:
    dmp_doi: str
    work_doi: str
    score: float
    work: WorkModel
    explanations: dict[str, Group]

    def to_dict(self) -> dict:
        return {
            "dmp_doi": self.dmp_doi,
            "work_doi": self.work_doi,
            "score": self.score,
            "work": self.work.model_dump(),
            "explanations": {name: group.to_dict() for name, group in self.explanations.items()},
        }


def msearch_dmp_works(
    client: OpenSearch,
    index_name: str,
    dmps: list[DMPModel],
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
) -> list[DMPWorksSearchResult]:
    # Execute searches
    body = []
    for dmp in dmps:
        body.append({})
        body.append(build_query(dmp, max_results, project_end_buffer_years))

    responses = client.msearch(
        body=body,
        index=index_name,
        max_concurrent_searches=max_concurrent_searches,
        max_concurrent_shard_requests=max_concurrent_shard_requests,
    )

    # Collate results
    results = []
    for i, response in enumerate(responses["responses"]):
        dmp = dmps[i]
        hits = response.get("hits", {}).get("hits", [])
        results.extend(collate_results(dmp, hits))

    return results


def search_dmp_works(
    client: OpenSearch,
    index_name: str,
    dmp: DMPModel,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    include_named_queries_score: bool = False,
) -> list[DMPWorksSearchResult]:
    body = build_query(dmp, max_results, project_end_buffer_years)
    response = client.search(
        body=body,
        index=index_name,
        include_named_queries_score=include_named_queries_score,
    )
    hits = response.get("hits", {}).get("hits", [])
    return collate_results(dmp, hits)


def collate_results(dmp: DMPModel, hits: list[dict]) -> list[DMPWorksSearchResult]:
    results: list[DMPWorksSearchResult] = []
    for hit in hits:
        work_doi: str = hit.get("_id")
        score: float = hit.get("_score", 0.0)
        work = WorkModel.model_validate(hit.get("_source", {}))
        matched_queries = hit.get("matched_queries")
        highlights = hit.get("highlight")
        explanation = explain_match(dmp, work, matched_queries, highlights)
        results.append(DMPWorksSearchResult(dmp.doi, work_doi, score, work, explanation))
    return results


################
# Query builder
################


def name_group(group: str) -> str:
    return json.dumps({"g": group, "lvl": "group"}, separators=(",", ":"))


def name_entity(group: str, idx: int) -> str:
    return json.dumps({"g": group, "lvl": "entity", "i": idx}, separators=(",", ":"))


def name_value(group: str, idx: int, key: str, value: str) -> str:
    return json.dumps({"g": group, "lvl": "value", "i": idx, "k": key, "v": value}, separators=(",", ":"))


def build_query(dmp: DMPModel, max_results: int, project_end_buffer_years: int) -> dict:
    should = []

    # Funded DOIs
    if dmp.funded_dois:
        should.append(
            {
                "constant_score": {
                    "_name": name_group("funded_dois"),
                    "boost": 15,
                    "filter": {
                        "ids": {"values": dmp.funded_dois},
                    },
                }
            }
        )

    # Award IDs
    award_queries = []
    group = "awards"
    for idx, award in enumerate(dmp.external_data.awards):
        queries = []
        for award_id in award.award_id.all_variants:
            queries.append(
                {
                    "constant_score": {
                        "_name": name_value(group, idx, "award_id", award_id),
                        "filter": {"term": {"award_ids": award_id}},
                        "boost": 10,
                    }
                }
            )
        award_queries.append(
            {
                "dis_max": {
                    "_name": name_entity(group, idx),
                    "tie_breaker": 0,
                    "queries": queries,
                }
            }
        )
    if len(award_queries):
        should.append(
            {
                "bool": {
                    "_name": name_group(group),
                    "minimum_should_match": 1,
                    "should": award_queries,
                },
            }
        )

    # Authors
    # Combines author_orcids and author surnames into a single feature
    authors = build_entity_query(
        "authors",
        "author_orcids",
        "author_names",
        dmp.authors,
        lambda author: author.orcid,
        lambda author: author.surname,
    )
    if authors is not None:
        should.append(authors)

    # Institutions
    # Combines both affiliation_rors and affiliation_names into a single feature
    institutions = build_entity_query(
        "institutions",
        "affiliation_rors",
        "affiliation_names",
        dmp.institutions,
        lambda inst: inst.ror,
        lambda inst: inst.name,
    )
    if institutions is not None:
        should.append(institutions)

    # Funders
    # Combines both funder_ids and funder_names into a single feature
    funders = build_entity_query(
        "funders",
        "funder_ids",
        "funder_names",
        dmp.funding,
        lambda fund: fund.funder.id,
        lambda fund: fund.funder.name,
    )
    if funders is not None:
        should.append(funders)

    # Title and abstract
    has_text = dmp.title is not None or dmp.abstract is not None
    content: str = " ".join([text for text in [dmp.title, dmp.abstract] if text is not None and text != ""])
    if has_text:
        should.append(
            {
                "more_like_this": {
                    "_name": name_group("content"),
                    "fields": ["title", "abstract"],
                    "like": content,
                    "min_term_freq": 1,
                }
            }
        )

    # Final query and filter based on date range
    # also remove DMPs from search results (output-management-plan type)
    filters = [
        {
            "bool": {
                "must_not": {
                    "term": {
                        "type": "output-management-plan",
                    }
                }
            },
        }
    ]
    if dmp.project_start is not None:
        gte = dmp.project_start.format("YYYY-MM-DD")
        lte = dmp.project_end.add(years=project_end_buffer_years).format("YYYY-MM-DD")
        filters.append(
            {
                "range": {
                    "publication_date": {
                        "gte": gte,
                        "lte": lte,
                    },
                }
            }
        )

    query = {
        "size": max_results,
        "query": {
            "bool": {
                "should": should,
                "filter": filters,
                "minimum_should_match": 1,
            },
        },
    }

    if has_text:
        query["highlight"] = {
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
            "order": "score",
            "require_field_match": True,
            "fields": {
                "title": {
                    "type": "fvh",
                    "number_of_fragments": 0,
                },
                "abstract": {
                    "type": "fvh",
                    "fragment_size": 160,
                    "number_of_fragments": 2,
                    "no_match_size": 160,
                },
            },
            "highlight_query": {
                "more_like_this": {
                    "fields": ["title", "abstract"],
                    "like": content,
                    "min_term_freq": 1,
                }
            },
        }

    return query


def build_entity_query(
    group_name: str,
    id_field: str,
    name_field: str,
    items: list,
    id_accessor: Callable,
    name_accessor: Callable,
) -> Optional[dict]:
    should_queries = []

    for idx, item in enumerate(items):
        entity_queries = []
        entity_id = id_accessor(item)
        entity_name = name_accessor(item)

        if entity_id is not None:
            entity_queries.append(
                {
                    "constant_score": {
                        "_name": name_value(group_name, idx, "id", entity_id),
                        "filter": {"term": {id_field: entity_id}},
                        "boost": 2,
                    }
                }
            )

        if entity_name is not None:
            entity_queries.append(
                {
                    "constant_score": {
                        "_name": name_value(group_name, idx, "name", entity_name),
                        "filter": {"match_phrase": {name_field: {"query": entity_name, "slop": 3}}},
                        "boost": 1,
                    }
                }
            )

        if entity_queries:
            should_queries.append(
                {
                    "dis_max": {
                        "_name": name_entity(group_name, idx),
                        "tie_breaker": 0,
                        "queries": entity_queries,
                    },
                }
            )

    if should_queries:
        return {
            "bool": {
                "_name": name_group(group_name),
                "minimum_should_match": 1,
                "should": should_queries,
            }
        }

    return None
