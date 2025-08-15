import json

from opensearchpy import OpenSearch
from tqdm import tqdm

from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.work_model import WorkModel
from dmpworks.opensearch.explanations import explain_match, Group
from dmpworks.opensearch.utils import (
    make_opensearch_client,
    OpenSearchClientConfig,
    yield_dmps,
)
from dmpworks.utils import timed


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
        desc="Find DMP work matches with OpenSearch",
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
) -> list[tuple[str, str, float, WorkModel, list[Group]]]:
    # Execute search
    query = build_query(dmp, max_results, project_end_buffer_years)
    response = client.search(
        body=query,
        index=index_name,
        track_total_hits=True,
        include_named_queries_score=True,
        # explain=True,
    )

    # Process results
    results = []
    total_hits = response.get("hits", {}).get("total", {}).get("value", 0)
    max_score = response.get("hits", {}).get("max_score")
    hits = response.get("hits", {}).get("hits", [])
    for hit in hits:
        work_doi: str = hit.get("_id")
        score: float = hit.get("_score")
        work = WorkModel.model_validate(hit.get("_source"))
        matched_queries = hit.get("matched_queries")
        highlights = hit.get("highlight")
        explanation = explain_match(dmp, work, matched_queries, highlights)
        results.append((dmp.doi, work_doi, score, work, explanation))

    return results


def name_group(group: str) -> str:
    return json.dumps({"g": group, "lvl": "group"}, separators=(",", ":"))


def name_entity(group: str, idx: int) -> str:
    return json.dumps({"g": group, "lvl": "entity", "i": idx}, separators=(",", ":"))


def name_value(group: str, idx: int, key: str, value: str) -> str:
    return json.dumps({"g": group, "lvl": "value", "i": idx, "k": key, "v": value}, separators=(",", ":"))


def build_query(dmp: DMPModel, max_results: int, project_end_buffer_years: int) -> dict:
    should = []

    # Funded DOIs
    if len(dmp.funded_dois):
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
    # TODO: should be made to operate like funders
    group = "authors"
    should.append(
        {
            "bool": {
                "_name": name_group(group),
                "minimum_should_match": 1,
                "should": [
                    *[
                        {
                            "constant_score": {
                                "_name": name_value(group, idx, "orcid", orcid),
                                "boost": 2,
                                "filter": {
                                    "term": {
                                        "author_orcids": {
                                            "value": orcid,
                                        },
                                    },
                                },
                            }
                        }
                        for idx, orcid in enumerate(dmp.author_orcids)
                    ],
                    *[
                        {
                            "constant_score": {
                                "_name": name_value(group, idx, "surname", surname),
                                "boost": 1,
                                "filter": {
                                    "match_phrase": {
                                        "author_names": {
                                            "query": surname,
                                        }
                                    }
                                },
                            }
                        }
                        for idx, surname in enumerate(dmp.author_surnames)
                    ],
                ],
            },
        }
    )

    # Affiliations
    # Combines both affiliation_rors and affiliation_names into a single feature
    # TODO: should be made to operate like funders
    group = "affiliations"
    should.append(
        {
            "bool": {
                "_name": name_group(group),
                "minimum_should_match": 1,
                "should": [
                    *[
                        {
                            "constant_score": {
                                "_name": name_value(group, idx, "ror", ror),
                                "boost": 2,
                                "filter": {
                                    "term": {
                                        "affiliation_rors": {
                                            "value": ror,
                                        },
                                    },
                                },
                            }
                        }
                        for idx, ror in enumerate(dmp.affiliation_rors)
                    ],
                    *[
                        {
                            "constant_score": {
                                "_name": name_value(group, idx, "name", name),
                                "filter": {
                                    "match_phrase": {
                                        "affiliation_names": {
                                            "query": name,
                                            "slop": 3,
                                        },
                                    },
                                },
                            }
                        }
                        for idx, name in enumerate(dmp.affiliation_names)
                    ],
                ],
            },
        }
    )

    # Funders
    # Combines both funder_ids and funder_names into a single feature
    group = "funders"
    should.append(
        {
            "bool": {
                "_name": name_group(group),
                "minimum_should_match": 1,
                "should": [
                    *[
                        {
                            "dis_max": {
                                "_name": name_entity(group, idx),
                                "tie_breaker": 0,
                                "queries": [
                                    {
                                        "constant_score": {
                                            "_name": name_value(group, idx, "id", fund.funder.id),
                                            "filter": {"term": {"funder_ids": fund.funder.id}},
                                            "boost": 2,
                                        }
                                    },
                                    {
                                        "constant_score": {
                                            "_name": name_value(group, idx, "name", fund.funder.name),
                                            "filter": {
                                                "match_phrase": {"funder_names": {"query": fund.funder.name, "slop": 3}}
                                            },
                                            "boost": 1,
                                        }
                                    },
                                ],
                            },
                        }
                        for idx, fund in enumerate(dmp.funding)
                    ]
                ],
            }
        }
    )

    # Title and abstract
    content: str = " ".join([text for text in [dmp.title, dmp.abstract] if text is not None and text != ""])
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
    gte = dmp.project_start.format("YYYY-MM-DD")
    lte = dmp.project_end.add(years=project_end_buffer_years).format("YYYY-MM-DD")

    query = {
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
        "highlight": {
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
        },
    }

    return query


if __name__ == "__main__":
    dmp_match_search(
        "dmps-index",
        "works-index-demo-2",
        OpenSearchClientConfig(),
        max_results=5,
    )
