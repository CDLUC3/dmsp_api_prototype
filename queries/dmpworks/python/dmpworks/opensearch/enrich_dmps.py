import logging
from typing import Any, Generator, Optional

import pendulum
from opensearchpy import OpenSearch
from tqdm import tqdm

from dmpworks.dmp.enrichment import fetch_funded_dois, parse_award_text
from dmpworks.dmp.model import DMPModel, Award, ExternalData
from dmpworks.opensearch.utils import make_opensearch_client, OpenSearchClientConfig

log = logging.getLogger(__name__)


def yield_modified_dmps(
    client: OpenSearch,
    index_name: str,
    page_size: int = 500,
    scroll_time: str = "60m",
) -> Generator[tuple[int, DMPModel], Any, Any]:
    # Fetch all modified DMPs
    query = {
        "query": {
            "script": {
                "script": {
                    "lang": "painless",
                    "source": """
                        try {
                            if (doc['modified'].size() == 0) {
                                return false;
                            }

                            if (doc['external_data.updated'].size() == 0) {
                                return true;
                            }

                            long modifiedMillis = doc['modified'].value.toInstant().toEpochMilli();
                            long externalUpdatedMillis = doc['external_data.updated'].value.toInstant().toEpochMilli();

                            return modifiedMillis >= externalUpdatedMillis;

                        } catch (Exception e) {
                            return false;
                        }
                    """,
                }
            }
        }
    }

    response = client.search(
        index=index_name,
        body=query,
        scroll=scroll_time,
        size=page_size,
        track_total_hits=True,
    )
    scroll_id = response["_scroll_id"]
    total_hits = response["hits"]["total"]["value"]
    hits = response["hits"]["hits"]

    while hits:
        for doc in hits:
            source = doc['_source']
            source = DMPModel.model_validate(source)
            yield total_hits, source

        # Get next batch
        response = client.scroll(
            scroll_id=scroll_id,
            scroll=scroll_time,
        )
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]
    client.clear_scroll(scroll_id=scroll_id)


def enrich_dmps(
    index_name: str,
    client_config: OpenSearchClientConfig,
    page_size: int = 500,
    scroll_time: str = "60m",
    email: Optional[str] = None,
):
    client = make_opensearch_client(client_config)

    total_dmps = None
    with tqdm(
        total=0,
        desc="Enrich DMPs in OpenSearch",
        unit="doc",
    ) as pbar:
        for total_hits, dmp in yield_modified_dmps(
            client,
            index_name,
            page_size=page_size,
            scroll_time=scroll_time,
        ):
            if total_dmps is None:
                total_dmps = total_hits
                pbar.total = total_dmps

            log.debug(f"Fetch additional metadata for DMP: {dmp.doi}")
            awards = []
            for fund in dmp.funding:
                # Parse Award IDs, which can be found in both funding_opportunity_id
                # and award_id
                award_ids = parse_award_text(fund.funder.id, fund.funding_opportunity_id)
                award_ids.extend(parse_award_text(fund.funder.id, fund.award_id))
                award_ids = set(award_ids)

                # Fetch additional data for each award ID
                for award_id in award_ids:
                    dois = fetch_funded_dois(award_id, email=email)
                    awards.append(
                        Award(
                            funder=fund.funder,
                            award_id=award_id,
                            funded_dois=dois,
                        )
                    )

            log.debug(f"Save additional metadata for DMP: {dmp.doi}")
            external_data = ExternalData(updated=pendulum.now(tz="UTC"), awards=awards).model_dump()
            response = client.update(
                index=index_name,
                id=dmp.doi,
                body={"doc": {"external_data": external_data}},
            )
            result = response.get("result")
            log.debug(f"Result of saving DMP metadata: {dmp.doi} {result}")

            pbar.update(1)
