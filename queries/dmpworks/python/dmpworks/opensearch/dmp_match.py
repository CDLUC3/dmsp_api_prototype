from opensearchpy import OpenSearch
from tqdm import tqdm

from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.work_model import WorkModel
from dmpworks.opensearch.explanations import explain_match, Explanation
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
    filtered = []
    for match in all_matches:
        explanation = match[4]
        for exp in explanation:
            if exp.name == "authors" and len(exp.fields):
                filtered.append(match)
                break
    a = 1


def opensearch_dmp_match_search(
    client: OpenSearch,
    index_name: str,
    dmp: DMPModel,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
) -> list[tuple[str, str, float, WorkModel, list[Explanation]]]:
    # Execute search
    query = build_query(dmp, max_results, project_end_buffer_years)
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


def build_query(dmp: DMPModel, max_results: int, project_end_buffer_years: int) -> dict:
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
                                            "query": surname,
                                        }
                                    }
                                }
                                for surname in dmp.author_surnames
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


if __name__ == "__main__":
    dmp_match_search(
        "dmps-index",
        "works-index-demo-2",
        OpenSearchClientConfig(),
        max_results=5,
    )
