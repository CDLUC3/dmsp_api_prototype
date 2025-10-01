import logging
import math
import pathlib
from typing import Callable, Optional

import jsonlines
import pendulum
from opensearchpy import OpenSearch
from tqdm import tqdm

from dmpworks.model.dmp_model import Award, DMPModel
from dmpworks.model.related_work_model import ContentMatch, DoiMatch, DoiMatchSource, ItemMatch, RelatedWork
from dmpworks.model.work_model import WorkModel
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

    def write_works(works: list[RelatedWork], count: int):
        for work in works:
            writer.write(
                # Convert fields to CamelCase for database
                work.model_dump(
                    by_alias=True,
                    mode="json",
                )
            )
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


def msearch_dmp_works(
    client: OpenSearch,
    index_name: str,
    dmps: list[DMPModel],
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
) -> list[RelatedWork]:
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
) -> list[RelatedWork]:
    body = build_query(dmp, max_results, project_end_buffer_years)
    response = client.search(
        body=body,
        index=index_name,
        include_named_queries_score=include_named_queries_score,
    )
    hits = response.get("hits", {}).get("hits", [])
    return collate_results(dmp, hits)


def parse_matched_queries(matched_queries: list[str] | dict[str, float]) -> dict[str, float]:
    if isinstance(matched_queries, list):
        return {key: math.nan for key in matched_queries}
    return matched_queries


def collate_results(dmp: DMPModel, hits: list[dict]) -> list[RelatedWork]:
    results: list[RelatedWork] = []
    for hit in hits:
        work_doi = hit.get("_id")
        score: float = hit.get("_score", 0.0)
        work = WorkModel.model_validate(hit.get("_source", {}), by_name=True, by_alias=False)
        matched_queries = parse_matched_queries(hit.get("matched_queries", []))
        highlights = hit.get("highlight", {})

        # Construct DOI match
        doi_found = "funded_dois" in matched_queries
        sources = []
        if doi_found:
            for award in dmp.external_data.awards:
                if work_doi in award.funded_dois:
                    parent = award.award_id
                    awards = [parent] + award.award_id.related_awards
                    for child in awards:
                        if child.award_url() is not None:
                            parent_award_id = parent.identifier_string() if parent != child else None
                            sources.append(
                                DoiMatchSource(
                                    parent_award_id=parent_award_id,
                                    award_id=child.identifier_string(),
                                    award_url=child.award_url(),
                                )
                            )
        doi_match = DoiMatch(
            found=doi_found,
            score=matched_queries.get("funded_dois", 0.0),
            sources=sources,
        )

        # Construct content match (based on title and abstract)
        title_highlights = highlights.get("title", [])
        abstract_highlights = highlights.get("abstract", [])
        content_score = matched_queries.get("content", 0.0)
        content_matched = "content" in matched_queries
        content_match = ContentMatch(
            score=content_score,
            title_highlight=title_highlights[0] if title_highlights and content_matched else None,
            abstract_highlights=abstract_highlights if content_matched else [],
        )

        # Construct matches based on inner hits
        inner_hits = hit.get("inner_hits", {})
        author_matches = to_item_matches(inner_hits, "authors")
        institution_matches = to_item_matches(inner_hits, "institutions")
        funder_matches = to_item_matches(inner_hits, "funders")
        award_matches = to_item_matches(inner_hits, "awards")

        results.append(
            RelatedWork(
                dmp_doi=dmp.doi,
                work=work,
                score=score,
                doi_match=doi_match,
                content_match=content_match,
                author_matches=author_matches,
                institution_matches=institution_matches,
                funder_matches=funder_matches,
                award_matches=award_matches,
            )
        )
    return results


def to_item_matches(inner_hits: dict, hit_name: str) -> list[ItemMatch]:
    matches = []
    hits = inner_hits.get(hit_name, {}).get("hits", {}).get("hits", [])
    for hit in hits:
        offset = hit.get("_nested", {}).get("offset")
        score = hit.get("_score")
        matched_queries = parse_matched_queries(hit.get("matched_queries", []))
        fields = [field.replace(f"{hit_name}.", "") for field in matched_queries.keys()]
        matches.append(
            ItemMatch(
                index=offset,
                score=score,
                fields=fields,
            )
        )
    return matches


def build_query(dmp: DMPModel, max_results: int, project_end_buffer_years: int) -> dict:
    should = []

    # Funded DOIs
    if dmp.funded_dois:
        should.append(
            {
                "constant_score": {
                    "_name": "funded_dois",
                    "boost": 15,
                    "filter": {
                        "ids": {"values": dmp.funded_dois},
                    },
                }
            }
        )

    # Authors
    authors = build_entity_query(
        "authors",
        "authors.orcid",
        "authors.full",
        dmp.authors,
        lambda author: author.orcid,
        lambda author: author.surname,
    )
    if authors is not None:
        should.append(authors)

    # Institutions
    institutions = build_entity_query(
        "institutions",
        "institutions.ror",
        "institutions.name",
        dmp.institutions,
        lambda inst: inst.ror,
        lambda inst: inst.name,
    )
    if institutions is not None:
        should.append(institutions)

    # Funders
    funders = build_entity_query(
        "funders",
        "funders.ror",
        "funders.name",
        dmp.funding,
        lambda fund: fund.funder.ror,
        lambda fund: fund.funder.name,
    )
    if funders is not None:
        should.append(funders)

    # Awards
    awards = build_awards_query(
        "awards",
        dmp.external_data.awards,
    )
    if awards is not None:
        should.append(awards)

    # Title and abstract
    has_text = dmp.title is not None or dmp.abstract is not None
    content: str = " ".join([text for text in [dmp.title, dmp.abstract] if text is not None and text != ""])
    if has_text:
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
    if dmp.project_start is not None and dmp.project_start >= pendulum.date(1990, 1, 1):
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
                    "fragment_size": 0,
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
    path: str,
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
                        "_name": id_field,
                        "filter": {"term": {id_field: entity_id}},
                        "boost": 2,
                    }
                }
            )

        if entity_name is not None:
            entity_queries.append(
                {
                    "constant_score": {
                        "_name": name_field,
                        "filter": {"match_phrase": {name_field: {"query": entity_name, "slop": 3}}},
                        "boost": 1,
                    }
                }
            )

        if len(entity_queries) > 1:
            should_queries.append(
                {
                    "dis_max": {
                        "tie_breaker": 0,
                        "queries": entity_queries,
                    },
                }
            )
        elif len(entity_queries) == 1:
            should_queries.append(entity_queries[0])

    if should_queries:
        return {
            "nested": {
                "path": path,
                "query": {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": should_queries,
                    }
                },
                "inner_hits": {"name": path},
            },
        }

    return None


def build_awards_query(
    path: str,
    awards: list[Award],
) -> Optional[dict]:
    """The dis_max ensures that all the variants of a single award only contribute
    a maximum score of 10."""

    award_queries = []
    for award in awards:
        queries = []
        for award_id in award.award_id.all_variants:
            queries.append(
                {
                    "constant_score": {
                        "_name": "awards.award_id",
                        "filter": {"term": {"awards.award_id": award_id}},
                        "boost": 10,
                    }
                }
            )
        award_queries.append(
            {
                "dis_max": {
                    "tie_breaker": 0,
                    "queries": queries,
                }
            }
        )

    if len(award_queries):
        return {
            "nested": {
                "path": path,
                "query": {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": award_queries,
                    }
                },
                "inner_hits": {"name": path},
            },
        }

    return None
