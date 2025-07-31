import logging

from opensearchpy import OpenSearch

from dmpworks.dmp.model import DMPFlat

log = logging.getLogger(__name__)


def dmps_search(
    client: OpenSearch,
    index_name: str,
    dmps_batch: list[DMPFlat],
    max_results: int = 100,
    project_end_buffer: int = 3,
    explain: bool = False,
):
    """

    Args:
        client:
        index_name:
        dmps_batch:
        max_results:
        project_end_buffer: the number of years after the project end date to search.

    Returns:

    """

    # Build query body
    body = []
    for dmp in dmps_batch:
        # Header
        body.append({})

        # Query
        should = []

        # Funded DOIs
        should.append(
            {
                "terms": {
                    "doi": dmp.funded_dois,
                    "boost": 100,
                },
            }
        )

        # Award IDs
        weight = 20
        num_awards = 1
        should.append(
            {
                "function_score": {
                    "_name": "awards",
                    "query": {"terms": {"award_ids": dmp.award_ids}},
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
                                    {"match_phrase": {"author_names": {"query": name, "slop": 3}}}
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
                                {"terms": {"affiliation_rors": dmp.affiliation_rors}},
                                *[
                                    {"match_phrase": {"affiliation_names": {"query": name, "slop": 3}}}
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
                                {"terms": {"funder_ids": dmp.funder_ids}},
                                *[
                                    {"match_phrase": {"funder_names": {"query": name, "slop": 3}}}
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

        # Title
        # TODO: add term document
        # TODO: need to add "term_vector": "yes" to mapping for title and abstract
        should.append(
            {
                "more_like_this": {
                    "fields": ["title", "abstract"],
                    "like": " ".join([text for text in [dmp.title, dmp.abstract] if text is not None and text != ""]),
                    "min_term_freq": 1,
                }
            }
        )

        body.append(
            {
                "query": {
                    "bool": {
                        "should": should,
                        "filter": [
                            {
                                "range": {
                                    "publication_date": {
                                        "gte": dmp.project_start.format("YYYY-MM-DD"),
                                        "lte": dmp.project_end.add(years=project_end_buffer).format("YYYY-MM-DD"),
                                    },
                                }
                            },
                        ],
                        "minimum_should_match": 1,
                    },
                },
                "size": max_results,
                "explain": True,
            }
        )

    # Execute multi-search
    response = client.msearch(body=body, index=index_name)

    # Process results
    results = []
    for idx, result in enumerate(response['responses']):
        print(f"Results for query {idx + 1}:")
        for hit in result['hits']['hits']:
            id = hit['_id']
            score = hit['_score']
            source = hit['_source']
            results.append((id, score, source))
    #
    # for id, score, source in results:
    #     response = client.explain(
    #         index="my-index",
    #         id="1",
    #         body={
    #             "query": {
    #                 "bool": {"must": [{"match": {"title": "opensearch"}}, {"range": {"date": {"gte": "2022-01-01"}}}]}
    #             }
    #         },
    #     )
