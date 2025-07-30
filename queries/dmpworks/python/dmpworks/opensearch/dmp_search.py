import dataclasses
import logging
from typing import Optional

import pendulum
from opensearchpy import OpenSearch

log = logging.getLogger(__name__)


@dataclasses.dataclass
class DMP:
    dmp_id: str
    title: str
    abstract: str
    project_start: pendulum.Date
    project_end: pendulum.Date
    title: Optional[str]
    abstract: Optional[str]
    affiliation_rors: list[str]
    affiliation_ids: list[str]
    affiliation_names: list[str]
    author_names: list[str]
    author_orcids: list[str]
    award_ids: list[str]
    funder_ids: list[str]
    funder_names: list[str]
    funded_dois: list[str]


def dmps_search(
    client: OpenSearch,
    index_name: str,
    dmps_batch: list[DMP],
    max_results: int = 100,
    project_end_buffer: int = 3,
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
        body.append(
            {
                "bool": {
                    "should": [
                        # funded dois
                        {"terms": {"doi": dmp.funded_dois, "boost": 5}},
                        # title
                        {"more_like_this": {"fields": ["title"], "like": dmp.title or ""}},
                        # abstract
                        {"more_like_this": {"fields": ["abstract"], "like": dmp.abstract or ""}},
                        # affiliation_rors
                        {"terms": {"affiliation_rors": dmp.affiliation_rors}},
                        # affiliation_names
                        {
                            "match": {
                                "affiliation_names": {
                                    "query": " ".join(dmp.affiliation_names),
                                    "operator": "or",
                                    "fuzziness": "AUTO",
                                }
                            }
                        },
                        # author_names
                        {
                            "match": {
                                "author_names": {
                                    "query": " ".join(dmp.author_names),
                                    "operator": "or",
                                    "fuzziness": "AUTO",
                                }
                            }
                        },
                        # author_orcids
                        {"terms": {"author_orcids": dmp.author_orcids}},
                        # award_ids
                        {"terms": {"award_ids": dmp.award_ids}},
                        # funder_ids
                        {"terms": {"funder_ids": dmp.funder_ids}},
                        # funder_names
                        {
                            "match": {
                                "funder_names": {
                                    "query": " ".join(dmp.funder_names),
                                    "operator": "or",
                                    "fuzziness": "AUTO",
                                }
                            }
                        },
                    ],
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
                "size": max_results,
            }
        )

    # Execute multi-search
    response = client.msearch(body=body, index=index_name)

    # Process results
    for idx, result in enumerate(response['responses']):
        print(f"Results for query {idx + 1}:")
        for hit in result['hits']['hits']:
            print(hit['_source'])
