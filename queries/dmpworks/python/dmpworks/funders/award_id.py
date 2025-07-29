from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T", bound="FunderID")


class AwardID(ABC):
    parent_ror_ids: list = []  # The funder ROR IDs

    def __init__(self, text: str, fields: List[str]):
        self.text = text
        self.fields = fields
        self.discovered_ids = []

    @abstractmethod
    def fetch_additional_metadata(self):
        """Fetches additional metadata associated with the award ID"""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def generate_variants(self) -> List[str]:
        """Generates variants of the funder ID"""
        raise NotImplementedError("Please implement")

    @staticmethod
    @abstractmethod
    def parse(text: str | None) -> Optional[T]:
        """Parses a funder ID"""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def identifier_string(self) -> str:
        """The canonical identifier as a string"""
        raise NotImplementedError("Please implement")

    def parts(self) -> List[IdentifierPart]:
        """The parts that make up the ID"""
        parts = []
        for field in self.fields:
            value = getattr(self, field)
            parts.append(IdentifierPart(value, field.upper()))
        return parts

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        for field in self.fields:
            if getattr(self, field) != getattr(other, field):
                return False

        return True

    def __hash__(self):
        values = [getattr(self, field) for field in self.fields]
        return hash(tuple(values))

    def __repr__(self):
        class_name = self.__class__.__name__
        attrs = ", ".join(f"{field}={getattr(self, field)!r}" for field in self.fields)
        return f"{class_name}({attrs})"

    def to_dict(self) -> Dict:
        """Converts the funder ID into a dict to load into BigQuery"""
        return {
            "ror_ids": list(self.ror_ids),
            "identifier": self.identifier_string(),
            "text": self.text,
            "parts": [part.to_dict() for part in self.parts()],
            "discovered_ids": [identifier.to_dict() for identifier in self.discovered_ids],
            "variants": self.generate_variants(),
        }


@dataclass
class Identifier:
    id: str
    type: str

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "type": self.type,
        }


@dataclass
class IdentifierPart:
    value: str
    type: str

    def to_dict(self) -> Dict:
        return {
            "value": self.value,
            "type": self.type,
        }
