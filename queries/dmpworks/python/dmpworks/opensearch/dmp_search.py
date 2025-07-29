import pathlib
from typing import Any, Generator, Optional

from opensearchpy import OpenSearch
from tqdm import tqdm

from dmpworks.dmp.enrichment import fetch_funded_dois, parse_award_text
from dmpworks.dmp.model import DMPModel, DMPSearchModel
from dmpworks.opensearch.utils import count_records, load_dataset, make_opensearch_client, OpenSearchClientConfig
from dmpworks.utils import timed


def to_dmp_search_model(
    model: DMPModel,
    email: Optional[str] = None,
) -> DMPSearchModel:
    award_ids = set()
    funder_ids = set()
    funder_names = set()
    funded_dois = set()

    for fund in model.funding:
        # Parse Award IDs
        temp = parse_award_text(fund.funder.id, fund.funding_opportunity_id)
        temp.extend(parse_award_text(fund.funder.id, fund.award_id))
        temp = set(temp)

        # Fetch additional data for each award ID
        for award_id in temp:
            dois = fetch_funded_dois(award_id, email=email)
            for doi in dois:
                funded_dois.add(doi)
            award_ids.add(award_id)

        # Add funder ids and names
        if fund.funder is not None:
            if fund.funder.id is not None:
                funder_ids.add(fund.funder.id)

            if fund.funder.name is not None:
                funder_names.add(fund.funder.name)

    # Generate award ID variants and author names
    award_ids = [variant for award_id in award_ids for variant in award_id.generate_variants()]
    author_names = [name.surname if name.surname is not None else name.full for name in model.author_names]
    author_names = [name for name in author_names if name is not None]

    return DMPSearchModel(
        doi=model.doi,
        project_start=model.project_start,
        project_end=model.project_end,
        title=model.title,
        abstract=model.abstract,
        affiliation_rors=model.affiliation_rors,
        affiliation_names=model.affiliation_names,
        author_names=author_names,
        author_orcids=model.author_orcids,
        award_ids=award_ids,
        funder_ids=list(funder_ids),
        funder_names=list(funder_names),
        funded_dois=list(funded_dois),
    )


def yield_dmp_batches(
    in_dir: pathlib.Path,
    batch_size: int = 100,
    email: Optional[str] = None,
) -> Generator[list[DMPSearchModel], Any, None]:
    dataset = load_dataset(in_dir)
    filtered = []
    # Type hints are wrong, e.g. parameter isn't int_batch_size, it is batch_size
    for batch in dataset.to_batches(batch_size=batch_size):
        dmps = batch.to_pylist()
        for dmp in dmps:
            model = to_dmp_search_model(
                DMPModel.model_validate(dmp),
                email=email,
            )
            if len(model.award_ids):
                filtered.append(model)

            if len(filtered) >= 1:
                yield filtered
                filtered = []

    if len(filtered):
        yield filtered


@timed
def dmps_search(
    index_name: str,
    in_dir: pathlib.Path,
    client_config: OpenSearchClientConfig,
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    explain: bool = False,
    email: Optional[str] = None,
):
    client = make_opensearch_client(client_config)
    total_records = count_records(in_dir)
    with tqdm(
        total=total_records,
        desc="Search for matches to DMPs",
        unit="dmp",
    ) as pbar:
        for batch in yield_dmp_batches(
            in_dir,
            batch_size=batch_size,
            email=email,
        ):
            matches = opensearch_dmps_search(
                client,
                index_name,
                batch,
                max_results=max_results,
                project_end_buffer_years=project_end_buffer_years,
                explain=explain,
            )
            pbar.update(len(batch))


def opensearch_dmps_search(
    client: OpenSearch,
    index_name: str,
    dmps_batch: list[DMPSearchModel],
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    explain: bool = False,
) -> list[tuple[str, float, dict]]:

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
                "function_score": {
                    "_name": "funded_dois",
                    "query": {
                        "terms": {
                            "doi": dmp.funded_dois,
                            "boost": 100,
                        },
                    },
                    "boost_mode": "multiply",
                    "functions": [],
                }
            }
        )

        # Award IDs
        weight = 20
        num_awards = 1
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
        content = " ".join([text for text in [dmp.title, dmp.abstract] if text is not None and text != ""])
        should.append(
            {
                "function_score": {
                    "_name": "content",
                    "query": {
                        "more_like_this": {
                            "fields": ["title", "abstract"],
                            "like": content,
                            "min_term_freq": 1,
                        }
                    },
                    "boost_mode": "multiply",
                    "functions": [],
                }
            }
        )

        # Final query and filter based on date range
        gte = dmp.project_start.format("YYYY-MM-DD")
        lte = dmp.project_end.add(years=project_end_buffer_years).format("YYYY-MM-DD")
        body.append(
            {
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
                "size": max_results,
                "explain": explain,
            }
        )

    # Execute multi-search
    response = client.msearch(body=body, index=index_name)

    # Process results
    results = []
    for idx, result in enumerate(response['responses']):
        print(f"Results for query {idx + 1}:")
        for hit in result['hits']['hits']:
            work_doi = hit['_id']
            score = hit['_score']
            source = hit['_source']
            results.append((work_doi, score, source))

    return results


if __name__ == "__main__":
    dmps_search(
        "works-index-demo",
        pathlib.Path(""),
        OpenSearchClientConfig(),
        max_results=5,
        explain=True,
    )
