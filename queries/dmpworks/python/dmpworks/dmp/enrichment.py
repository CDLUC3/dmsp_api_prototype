from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional, Type

from dmpworks.dmp.model import DMPModel, Funder
from dmpworks.funders.award_id import AwardID
from dmpworks.funders.nih_award_id import NIHAwardID
from dmpworks.funders.nih_funder_api import nih_fetch_award_publication_dois
from dmpworks.funders.nsf_award_id import NSFAwardID
from dmpworks.funders.nsf_funder_api import nsf_fetch_award_publication_dois

log = logging.getLogger(__name__)


@dataclass(kw_only=True)
class Award:
    funder: Funder
    award_id: AwardID
    funded_works: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "funder": self.funder.to_dict(),
            "award_id": self.award_id.to_dict(),
            "funded_works": self.funded_works,
        }


def parse_awards(dmp: DMPModel) -> list[Award]:
    awards: list[Award] = []
    for funding in dmp.funding:
        # Parse award IDs
        award_ids = []
        if funding.funder.id is not None:
            award_ids = parse_award_ids(funding.funder.id, funding.funding_opportunity_id, funding.grant_id)

        # Add awards
        for award_id in award_ids:
            awards.append(
                Award(
                    funder=funding.funder,
                    award_id=award_id,
                )
            )
    return awards


def parse_award_ids(
    funder_id: str,
    funding_opportunity_id: Optional[str],
    grant_id: Optional[str],
) -> list[AwardID]:
    award_ids = set()
    parser_index: dict[str, Type[AwardID]] = {}
    for id_type in [NIHAwardID, NSFAwardID]:
        for ror_id in id_type.parent_ror_ids:
            parser_index[ror_id] = id_type
    parser: Optional[AwardID] = parser_index.get(funder_id)
    if parser:
        inputs = [funding_opportunity_id, grant_id]
        for text in inputs:
            if text is not None:
                # Handle cases where multiple awards specified, for example:
                # U19 AI111143; U19 AI111143
                # Lead 2126792, 2126793, 2126794, 2126795, 2126796, 2126797, 2126798, 2126799
                # Then parse each part
                parts = re.split(r"[;,]", text)
                for part in parts:
                    award_id = parser.parse(part)
                    if award_id is not None:
                        award_ids.add(award_id)

    return list(award_ids)


def update_award_data(
    awards: list[Award],
    email: Optional[str] = None,
):
    for award in awards:
        log.debug(f"Fetching data for award {award.award_id.text}")

        log.debug(f"Fetching additional metadata for award {award.award_id.text}")
        award.award_id.fetch_additional_metadata()

        canonical_id = award.award_id.identifier_string()
        log.debug(f"Canonical ID {canonical_id}")

        log.debug(f"Fetching funded works for award {canonical_id}")
        works = []
        award_id = award.award_id
        # Fetch NIH Award info
        if isinstance(award_id, NIHAwardID):
            log.debug(f"NIHAwardID fetch works via application IDs")
            for detail in award_id.nih_project_details:
                log.debug(f"Fetch works for {canonical_id} with appl_id={detail.appl_id}")
                results = nih_fetch_award_publication_dois(
                    detail.appl_id,
                    pubmed_api_email=email,
                )
                log.debug(f"Discovered {len(results)} associated works")
                works.extend(results)

        # Fetch NSF award info
        elif isinstance(award_id, NSFAwardID):
            log.debug(f"NSFAwardID fetch works for {canonical_id} with award_id={award_id.award_id}")
            results = nsf_fetch_award_publication_dois(
                award_id.award_id,
                email=email,
            )
            log.debug(f"Discovered {len(results)} associated works")
            works.extend(results)

        # Parse and set DOIs for funded works
        funded_works = set()
        for work in works:
            doi = work.get("doi")
            if doi is not None:
                funded_works.add(doi)
        award.funded_works = list(funded_works)
