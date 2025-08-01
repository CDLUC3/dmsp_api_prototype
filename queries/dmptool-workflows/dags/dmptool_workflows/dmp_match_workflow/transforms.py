from typing import Optional


def clean_doi(text: Optional[str]) -> str | None:
    if text is None:
        return None

    return text.lower().strip()
