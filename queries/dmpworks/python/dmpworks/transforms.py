import re
from typing import Optional


def extract_doi(text: str) -> Optional[str]:
    """Extract a DOI from a string using a regex.

    :param text: the text.
    :return: the DOI or None if no DOI was found.
    """

    pattern = r"10\.[\d.]+/[^\s]+"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return clean_string(match.group(0))
    return None


def clean_string(text: Optional[str]) -> str | None:
    if text is None:
        return None

    return text.lower().strip()
