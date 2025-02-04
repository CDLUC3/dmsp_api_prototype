from __future__ import annotations

import logging
import os
import re

import requests
from fold_to_ascii import fold
from observatory_platform.files import get_chunks
from observatory_platform.url_utils import retry_session
from rapidfuzz import fuzz


def get_pubmed_api_email():
    email = os.getenv("PUBMED_API_EMAIL_ADDRESS")
    if email is None:
        raise ValueError(f"get_pubmed_api_email: the PUBMED_API_EMAIL_ADDRESS is not set")


PUBMED_API_EMAIL_ADDRESS = get_pubmed_api_email()


def nsf_fetch_award_publication_dois(award_id: str, threshold: float = 95) -> list[dict]:
    """Fetch publications associated with an NSF award ID.

    :param award_id: the NSF award ID.
    :param threshold: when no DOI is specified, Crossref Metadata and DataCite are queried the minimum threshold to
    for title matching.
    :return: a list of works.
    """

    base_url = "https://www.research.gov/awardapi-service/v1/awards.json"
    params = {"id": award_id, "printFields": "publicationResearch"}

    try:
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Collect references
        references = []
        awards = data.get("response", {}).get("award", [])
        for award in awards:
            publication_research = award.get("publicationResearch", [])
            references.extend(publication_research)

        # Parse references
        references = [parse_reference(ref) for ref in references]

        # Attempt to find missing DOIs from Crossref Metadata and DataCite
        for ref in references:
            doi = ref.get("doi")
            title = ref.get("title")
            journal = ref.get("journal")

            # Try Crossref Metadata first
            if doi is None:
                doi = find_crossref_doi(title, journal, threshold=threshold)

            # If DOI is still None, then try DataCite
            if doi is None:
                doi = find_datacite_doi(title, threshold=threshold)

            ref["doi"] = doi

        return references

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while fetching data: {e}")
        raise


def find_crossref_doi(title: str, journal: str, threshold: float = 95) -> str | None:
    """Given an academic work title and a journal name, fetch a list of candidate DOIs from Crossref Metadata
    and accept the title with a similarity greater than or equal to the threshold.

    :param title: the title.
    :param journal: the journal name.
    :param threshold: the minimum threshold for accepting a match.
    :return: the DOI or None.
    """

    base_url = "https://api.crossref.org/works"
    params = {"query.title": title, "query.container-title": journal}

    try:
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()

        data = response.json()
        items = data.get("message", {}).get("items", [])

        for item in items:
            # Get title for item
            item_title = item.get("title")
            item_title = item_title[0] if isinstance(item_title, list) and item_title else ""

            # Accept title if similarity >= threshold
            if fuzz.ratio(title, item_title, processor=preprocess_text) >= threshold:
                return item.get("DOI")

        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"find_crossref_doi: an error occurred while fetching data: {e}")
        raise


def find_datacite_doi(title: str, threshold: float = 95) -> str | None:
    """Given an academic work title, fetch a list of candidate DOIs from DataCite
    and accept the title with a similarity greater than or equal to the threshold.

    :param title: the title.
    :param threshold: the minimum threshold for accepting a match.
    :return: the DOI or None.
    """

    base_url = "https://api.datacite.org/dois"
    title_quoted = title.replace('"', '\\"')
    params = {"query": f'titles.title:"{title_quoted}"', "sort": "relevance"}
    try:
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()

        data = response.json()
        items = data.get("data")
        for item in items:
            doi = item.get("id")
            attributes = item.get("attributes", {})

            # Get title for item
            item_titles = attributes.get("titles", [])  # Example: titles [{'title': 'COKI Open Access Dataset'}]
            item_title = item_titles[0].get("title") if item_titles else ""

            # Accept title if similarity >= threshold
            if fuzz.ratio(title, item_title, processor=preprocess_text) >= threshold:
                # Check to see if there is a root version of record
                # Example: relatedIdentifiers [{'relatedIdentifier': '10.5281/zenodo.6399462', 'relatedIdentifierType': 'DOI', 'relationType': 'IsVersionOf'}, {'relatedIdentifier': 'https://zenodo.org/communities/coki', 'relatedIdentifierType': 'URL', 'relationType': 'IsPartOf'}]
                related_identifiers = attributes.get("relatedIdentifiers")
                for related in related_identifiers:
                    related_identifier = related.get("relatedIdentifier")
                    if related.get("relationType") == "IsVersionOf" and related_identifier:
                        doi = related_identifier
                        break

                return doi

        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"find_crossref_doi: an error occurred while fetching data: {e}")
        raise


def preprocess_text(text) -> str:
    """Pre-process text before similarity matching. Converts to lower case, folds non-ASCII characters into ASCII
    characters, removes punctuation and normalises spaces.

    :param text: the text to preprocess.
    :return: the preprocessed text.
    """

    text = text.lower()  # Convert to lowercase
    text = fold(text)  # Fold non-ASCII characters into ASCII
    text = re.sub(
        r"[^\w\s]", "", text
    )  # Remove punctuation: replaces any character that is not a word or whitespace with ""
    text = re.sub(
        r"\s+", " ", text
    ).strip()  # Normalise spaces by replacing whitespace character with one or more occurrences with a single space
    return text


def parse_reference(reference: str) -> dict:
    """Parse an NSF award publication reference.

    :param reference: the reference string.
    :return: a dictionary with doi, journal, year, title and reference set.
    """

    # Split on ~
    parts = reference.split("~")

    # Journal: always first
    journal = parts[0].strip() if len(parts) > 0 else None

    # Year: always second
    year = parts[1].strip() if len(parts) > 1 else None

    # Parse DOI and title in reverse
    doi = None
    title = None
    parts.reverse()
    for i, part in enumerate(parts):
        # Title is always preceded by a date or the N character
        if title is None and i >= 1 and part.strip().lower() != "n":
            title = part

        # Check every part for a DOI
        if doi is None:
            doi = extract_doi(part)

        # Break early if both found
        if title is not None and doi is not None:
            break

    return dict(
        doi=doi,
        journal=journal,
        year=year,
        title=title,
        reference=reference,
    )


def extract_doi(text: str) -> str | None:
    """Extract a DOI from a string using a regex.

    :param text: the text.
    :return: the DOI or None if no DOI was found.
    """

    pattern = r"10\.[\d.]+/[^\s]+"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).lower().strip()
    return None


def nih_core_project_to_appl_ids(core_project_num: str) -> list[str]:
    """Get the NIH Application IDs associated with an NIH Core Project Number.

    :param core_project_num: the NIH Core Project Number.
    :return: the list of NIH Application IDs.
    """

    try:
        base_url = "https://api.reporter.nih.gov/v2/projects/search"
        data = {"criteria": {"project_nums": [core_project_num]}, "include_fields": ["ApplId", "ProjectNum"]}
        response = retry_session().post(base_url, json=data)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return [result["appl_id"] for result in results]

    except requests.exceptions.RequestException as e:
        logging.error(f"nih_fetch_award_publication_dois: an error occurred while fetching data: {e}")
        raise


def nih_fetch_award_publication_dois(appl_id: str, pubmed_api_email: str = PUBMED_API_EMAIL_ADDRESS) -> list[dict]:
    """Fetch the publications associated with an NIH award.

    :param appl_id: the NIH Application ID.
    :param pubmed_api_email: an email address to use when calling the PubMed API.
    :return: a list of publication DOIs.
    """

    base_url = "https://reporter.nih.gov/services/Projects/Publications"
    params = {
        "projectId": appl_id,
    }

    try:
        # Fetch list of NIH award publications
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        rows = data.get("results", [])

        # Get the PubMed IDs and PMC IDs
        pm_ids = []
        pmc_ids = []
        for row in rows:
            pm_id = row.get("pm_id")
            pmc_id = row.get("pmc_id")
            if pm_id is not None:
                pm_ids.append(pm_id)
            elif pmc_id is not None:
                pmc_ids.append(pm_id)

        # Add DOIs to outputs
        outputs = []
        if len(pm_ids) > 0:
            outputs.extend(pubmed_ids_to_dois(pm_ids, "pmid", email=pubmed_api_email))
        if len(pmc_ids) > 0:
            outputs.extend(pubmed_ids_to_dois(pmc_ids, "pmcid", email=pubmed_api_email))
        return outputs

    except requests.exceptions.RequestException as e:
        logging.error(f"nih_fetch_award_publication_dois: an error occurred while fetching data: {e}")
        raise


def pubmed_ids_to_dois(
    ids: list[int],
    idtype: str,
    versions: str | None = "no",
    tool: str = "dmptool-workflow",
    email: str = PUBMED_API_EMAIL_ADDRESS,
) -> list[dict]:
    """Call the PubMed ID converter API to convert PubMed IDs and PMC IDs to DOIs: https://pmc.ncbi.nlm.nih.gov/tools/id-converter-api/

    :param ids: a list of PubMed IDs or PMC IDs.
    :param idtype: what type of IDs are supplied: "pmcid", "pmid", "mid", or "doi".
    :param versions: whether to return version information.
    :param tool: the name of the tool.
    :param email: an email address to set in the PubMed API.
    :return:
    """

    batches = get_chunks(input_list=ids, chunk_size=200)
    outputs = []
    for batch in batches:
        outputs.extend(_pubmed_ids_to_dois(batch, idtype, versions, tool, email))
    return outputs


def _pubmed_ids_to_dois(
    ids: list[int],
    idtype: str,
    versions: str | None = "no",
    tool="dmptool-match-workflows",
    email: str = PUBMED_API_EMAIL_ADDRESS,
) -> list[dict]:
    # Validate parameters
    if len(ids) > 200:
        raise ValueError(f"pubmed_id_converter: a maximum of 200 IDs can be supplied at once")

    if idtype not in {"pmcid", "pmid", "mid", "doi"}:
        raise ValueError(
            f"pubmed_id_converter: incorrect idtype {idtype}, should be one of 'pmcid', 'pmid', 'mid', or 'doi'"
        )

    if versions not in {None, "no"}:
        raise ValueError(f"pubmed_id_converter: versions should be None or 'no', not {versions}")

    # Construct params
    base_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
    params = {
        "ids": ",".join([str(id) for id in ids]),
        "format": "json",
        "idtype": idtype,
        "tool": tool,
        "email": email,
    }
    if versions:
        params["versions"] = versions

    try:
        # Fetch data
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        records = data.get("records", [])

        # Format outputs
        outputs = []
        for record in records:
            pmcid = record.get("pmcid")
            pmid = record.get("pmid")
            doi = record.get("doi")
            outputs.append(dict(pmcid=pmcid, pmid=pmid, doi=doi))
        return outputs

    except requests.exceptions.RequestException as e:
        logging.error(f"pubmed_id_converter: an error occurred while fetching data: {e}")
        raise
