from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, TypeVar

from dmptool_workflows.dmp_match_workflow.funder_apis import (
    nih_core_project_to_appl_ids,
    NIHProjectDetails,
    nsf_fetch_org_id,
)
from dmptool_workflows.dmp_match_workflow.types import Identifier, IdentifierPart

T = TypeVar("T", bound="FunderID")


class FunderID(ABC):
    ror_id: str = None  # The funders ROR ID

    def __init__(self, text: str):
        self.text = text
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

    @abstractmethod
    def parts(self) -> List[IdentifierPart]:
        """The parts that make up the ID"""
        raise NotImplementedError("Please implement")

    def to_dict(self) -> Dict:
        """Converts the funder ID into a dict to load into BigQuery"""
        return {
            "identifier": self.identifier_string(),
            "text": self.text,
            "parts": [part.to_dict() for part in self.parts()],
            "discovered_ids": [identifier.to_dict() for identifier in self.discovered_ids],
            "variants": self.generate_variants(),
        }


class NIHAwardID(FunderID):
    ror_id = "01cwqze88"

    def __init__(
        self,
        *,
        text: str,
        application_type: Optional[str] = None,
        activity_code: Optional[str] = None,
        institute_code: Optional[str] = None,
        serial_number: Optional[str] = None,
        support_year: Optional[str] = None,
        other_suffixes: Optional[str] = None,
    ):
        """Construct an NIH Award ID. Details in docstrings derived from
         * https://www.era.nih.gov/files/Deciphering_NIH_Application.pdf
         * https://grants.nih.gov/funding/activity-codes
         * https://pmc.ncbi.nlm.nih.gov/articles/PMC3495907/

        :param text: the original text that the ID was parsed from.
        :param application_type: classifies funding requests based on their purpose, such as new projects, renewals,
        revisions, extensions, continuations, and administrative changes like transfers between institutions, organizations,
        or NIH institutes.
        :param activity_code: NIH uses three-character activity codes to categorize research-related programs, with the
        first character indicating the funding type (e.g., "R" for research, "T" for training), though their use may vary
        by institute or center.
        :param institute_code: the NIH institution code.
        :param serial_number: six‐digit number assigned within an Institute/Center.
        :param support_year: two‐digit number indicating segment or budget period of a project.
        :param other_suffixes: "A" and related number identifies the amendment number (e.g. A1 = resubmission); "S" and
        related number identifies the revision record and follows the grant year or the amendment designation to which
        additional funds have been awarded.
        """

        super(NIHAwardID).__init__(text)

        self.application_type = application_type
        self.activity_code = activity_code
        self.institute_code = institute_code
        self.serial_number = serial_number
        self.support_year = support_year
        self.other_suffixes = other_suffixes
        self.text = text
        self.nih_project_details: List[NIHProjectDetails] = []
        self.discovered_ids: List[Identifier] = []

    def __eq__(self, other):
        if isinstance(other, NIHAwardID):
            return (
                self.application_type == other.application_type
                and self.activity_code == other.activity_code
                and self.institute_code == other.institute_code
                and self.serial_number == other.serial_number
                and self.support_year == other.support_year
                and self.other_suffixes == other.other_suffixes
            )
        return False

    def __str__(self):
        # TODO: could turn this into a data class and get this for free?
        parts = []
        if self.application_type:
            parts.append(f"application_type={self.application_type}")

        if self.activity_code:
            parts.append(f"activity_code={self.activity_code}")

        if self.institute_code:
            parts.append(f"institute_code={self.institute_code}")

        if self.serial_number:
            parts.append(f"serial_number={self.serial_number}")

        if self.support_year:
            parts.append(f"support_year={self.support_year}")

        if self.other_suffixes:
            parts.append(f"other_suffixes={self.other_suffixes}")

        return f"NIHAwardID({', '.join(parts)})"

    def identifier_string(self) -> str:
        """The canonical identifier as a string"""

        parts = []
        if self.application_type:
            parts.append(self.application_type)

        if self.activity_code:
            parts.append(self.activity_code)

        if self.institute_code:
            parts.append(self.institute_code)

        if self.serial_number:
            parts.append(self.serial_number)

        if self.support_year or self.other_suffixes:
            parts.append("-")

            if self.support_year:
                parts.append(self.support_year)

            if self.other_suffixes:
                parts.append(self.other_suffixes)

        return "".join(parts)

    def parts(self) -> List[IdentifierPart]:
        """The parts that make up the ID"""

        parts = []

        if self.application_type:
            parts.append(IdentifierPart(self.application_type, "APPLICATION_TYPE"))

        if self.activity_code:
            parts.append(IdentifierPart(self.activity_code, "ACTIVITY_CODE"))

        if self.institute_code:
            parts.append(IdentifierPart(self.institute_code, "INSTITUTION_CODE"))

        if self.serial_number:
            parts.append(IdentifierPart(self.serial_number, "SERIAL_NUMBER"))

        if self.support_year:
            parts.append(IdentifierPart(self.support_year, "SUPPORT_YEAR"))

        if self.other_suffixes:
            parts.append(IdentifierPart(self.other_suffixes, "OTHER_SUFFIXES"))

        return parts

    def generate_variants(self) -> List[str]:
        all_award_ids = [self] + [NIHAwardID.parse(detail.project_num) for detail in self.nih_project_details]
        variants = set()
        for award_id in all_award_ids:
            variants.update(nih_awards_generate_variants(award_id))
        return list(variants)

    def fetch_additional_metadata(self):
        # Convert award ID to application IDs
        self.nih_project_details = nih_core_project_to_appl_ids(
            appl_type_code=self.application_type,
            activity_code=self.activity_code,
            ic_code=self.institute_code,
            serial_num=self.serial_number,
            support_year=self.support_year,
            suffix_code=self.other_suffixes,
        )

        # Add these to the discovered IDs to save into BigQuery
        for obj in self.nih_project_details:
            self.discovered_ids.append(Identifier(id=obj.appl_id, type="NIH_APPL_ID"))
            self.discovered_ids.append(Identifier(id=obj.project_num, type="NIH_PROJECT_NUM"))

    @staticmethod
    def parse(text: str | None) -> Optional[NIHAwardID]:
        return parse_nih_award_id(text)


def parse_nih_award_id(text: str | None) -> Optional[NIHAwardID]:
    """Parse an NIH award ID string into an NIHAwardID object.

    :param text: the text containing the NIH award ID.
    :return: the NIHAwardID object or None if an award ID could not be matched.
    """

    original_text = text

    # Return None if None or empty string
    if text is None or text.strip() == "":
        return None

    # Uppercase, remove hyphens and spaces
    # 1 R01 AG080054-01R1 -> 1R01AG08005401R1
    text = text.upper().replace("-", "").replace(" ", "").strip()

    # Look for two letter institution code, followed by six digit serial number
    # If we don't find at least this pattern, then don't consider it to be an NIH award ID
    main_pattern = re.compile(r"(?P<institute_code>[A-Z]{2})(?P<serial_number>\d{6})")
    match = main_pattern.search(text)
    if not match:
        return None

    # Get institute_code and serial_number and split into prefix and suffix
    institute_code = match.group("institute_code")  # AG
    serial_number = match.group("serial_number")  # 080054
    prefix = text[: match.start()]  # 1R01
    suffix = text[match.end() :]  # 01R1

    # Try to parse application_type and activity_code
    application_type = None
    activity_code = None
    if prefix:
        # Fix common typos and errors in prefix
        prefix = prefix.replace("RO1", "R01")
        prefix = prefix.replace("NIH", "")

        # If there is only one character, and it is a digit from 1-9 then set application_type
        if len(prefix) == 1 and re.fullmatch(r"[1-9]", prefix):
            application_type = prefix  # 1
        # If there are three characters, and they are a mix of digits and numbers, then set activity_code
        elif len(prefix) == 3 and re.fullmatch(r"[\dA-Z]{3}", prefix):
            activity_code = prefix  # R01
        # If there are four or more characters, then try to match both at same time
        elif len(prefix) >= 4:
            pattern = re.compile(r"^(?P<application_type>[1-9])(?P<activity_code>[\dA-Z]{3})$")
            match = pattern.match(prefix)
            if match:
                application_type = match.group("application_type")  # 1
                activity_code = match.group("activity_code")  # R01

    # Try to parse support_year and other_suffixes
    support_year = None
    other_suffixes = None
    if suffix:
        # If two characters, then check if they are both digits, if so, then they are the support year.
        # otherwise they are the other suffixes
        if len(suffix) == 2:
            if re.fullmatch(r"[\d]{2}", suffix):
                support_year = suffix  # 01
            else:
                other_suffixes = suffix  # R1
        # Otherwise try a full pattern match
        elif len(suffix) >= 3:
            pattern = re.compile(r"^(?P<support_year>[\d]{2})(?P<other_suffixes>[\dA-Z]{2,4})$")
            match = pattern.match(suffix)
            if match:
                support_year = match.group("support_year")  # 01
                other_suffixes = match.group("other_suffixes")  # R1

    return NIHAwardID(
        application_type=application_type,
        activity_code=activity_code,
        institute_code=institute_code,
        serial_number=serial_number,
        support_year=support_year,
        other_suffixes=other_suffixes,
        text=original_text,
    )


def nih_awards_generate_variants(award_id: NIHAwardID) -> Set[str]:
    """Generates different string representations of an NIH Award ID.

    :param award_id: the NIH Award ID.
    :return: variants.
    """

    base = f"{award_id.institute_code} {award_id.serial_number}"
    variants = {base.replace(" ", ""), base}  # Include "AI176039" and "AI 176039"

    # Support year suffix
    if award_id.support_year:
        base_with_support_year = f"{base}-{award_id.support_year}"
        variants.update({base_with_support_year.replace(" ", ""), base_with_support_year})

    # Support year and other suffixes
    if award_id.support_year and award_id.other_suffixes:
        base_with_support_year_other_suffixes = f"{base}-{award_id.support_year}{award_id.other_suffixes}"
        variants.update({base_with_support_year_other_suffixes.replace(" ", ""), base_with_support_year_other_suffixes})

    # Activity code prefix variants
    if award_id.activity_code:
        for variant in list(variants):  # Extend all base variants with activity_code
            variants.add(f"{award_id.activity_code} {variant}")
            variants.add(f"{award_id.activity_code}{variant.replace(' ', '')}")

    # Application type prefix variants
    if award_id.application_type:
        for variant in list(variants):  # Extend all current variants with application_type
            variants.add(f"{award_id.application_type} {variant}")
            variants.add(f"{award_id.application_type}{variant.replace(' ', '')}")

    return variants


@dataclass
class NSFAwardID(FunderID):
    ror_id = "021nxhr62"

    def __init__(self, *, text: str, org_id: Optional[str] = None, award_id: Optional[str] = None):
        """Construct an NSF Award ID.

        :param org: NSF org ID, e.g. IOS, CBET etc.
        :param award_id: 7 digit award ID, .e.g 1509218.
        :param text: the original text it was parsed from.
        """

        super(NSFAwardID).__init__(text)
        self.org_id = org_id
        self.award_id = award_id
        self.discovered_ids: List[Identifier] = []

    def __eq__(self, other):
        if isinstance(other, NSFAwardID):
            return self.org_id == other.org_id and self.award_id == other.award_id
        return False

    def __str__(self):
        # TODO: could turn this into a data class and get this for free?
        parts = []
        if self.org_id:
            parts.append(f"org_id={self.org_id}")

        if self.award_id:
            parts.append(f"award_id={self.award_id}")

        return f"NSFAwardID({', '.join(parts)})"

    def generate_variants(self):
        variants = []

        # 1507101
        if self.award_id is not None:
            variants.append(str(self.award_id))

        if self.org_id is not None and self.award_id is not None:
            # DMR 1507101
            variants.append(f"{self.org_id} {self.award_id}")

            # DMR-1507101
            variants.append(f"{self.org_id}-{self.award_id}")

        return variants

    def fetch_additional_metadata(self):
        """Fetch the NSF Org ID for the award ID.

        :return: None.
        """

        if self.org_id is None and self.award_id is not None:
            self.org_id = nsf_fetch_org_id(self.award_id)
            if self.org_id is not None:
                self.discovered_ids = [Identifier(f"{self.org_id}-{self.award_id}", "NSF_AWARD_ID")]

    def identifier_string(self) -> str:
        """The canonical identifier as a string"""

        if self.org_id and self.award_id:
            return f"{self.org_id}-{self.award_id}"

        return str(self.award_id)

    def parts(self) -> List[IdentifierPart]:
        """The parts that make up the ID"""

        parts = []

        if self.org_id:
            parts.append(IdentifierPart(self.org_id, "ORG_ID"))

        if self.award_id:
            parts.append(IdentifierPart(self.award_id, "AWARD_ID"))

        return parts

    @staticmethod
    def parse(text: str | None) -> Optional[NSFAwardID]:
        return parse_nsf_award_id(text)


def parse_nsf_award_id(text: str | None) -> Optional[NSFAwardID]:
    original_text = text

    # Return None if None or empty string
    if text is None or text.strip() == "":
        return None

    # Try to parse NSF award URL AWD_ID query parameter
    # https://www.nsf.gov/awardsearch/showAward?AWD_ID=2234213&HistoricalAwards=false
    match = re.search(r"AWD_ID=(?P<award_id>\d+)", text)
    if match:
        return NSFAwardID(award_id=match.group("award_id"), text=original_text)

    # Fixup common typos and errors
    text = text.replace("NSF-", "")
    text = text.replace("NSF", "")
    text = text.strip()

    # Try to parse org_id and award_id together
    match = re.search(r"(?P<org_id>[A-Z]{3,4})(?P<award_id>\d{7})", text)
    if match:
        return NSFAwardID(org_id=match.group("org_id"), award_id=match.group("award_id"), text=original_text)

    # Try to parse 7 digit award_id by itself
    match = re.search(r"\d{7}", text)
    if match:
        return NSFAwardID(award_id=match.group(), text=original_text)

    return None
