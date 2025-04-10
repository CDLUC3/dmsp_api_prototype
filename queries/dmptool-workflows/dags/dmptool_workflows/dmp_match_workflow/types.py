from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from dmptool_workflows.dmp_match_workflow.funder_ids import FunderID


@dataclass(kw_only=True)
class DMP:
    dmp_id: str
    funding: List[Fund] = field(default_factory=list)
    awards: List[Award] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "dmp_id": self.dmp_id,
            "funding": [fund.to_dict() for fund in self.funding],
            "awards": [award.to_dict() for award in self.awards],
        }


@dataclass(kw_only=True)
class Fund:
    funder: Funder
    funding_opportunity_id: Optional[str] = None
    grant_id: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "funder": self.funder.to_dict(),
            "funding_opportunity_id": self.funding_opportunity_id,
            "grant_id": self.grant_id,
        }


@dataclass(kw_only=True)
class Award:
    funder: Funder
    award_id: FunderID
    funded_works: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {"funder": self.funder.to_dict(), "award_id": self.award_id.to_dict(), "funded_works": self.funded_works}


@dataclass(kw_only=True)
class Funder:
    id: Optional[str] = None
    name: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
        }
