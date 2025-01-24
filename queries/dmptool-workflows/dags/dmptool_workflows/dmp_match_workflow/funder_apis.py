import logging

import requests
from observatory_platform.files import get_chunks
from observatory_platform.url_utils import retry_session

EMAIL_ADDRESS = "agent@observatory.academy"


def nsf_fetch_award_publication_dois(award_id: str) -> list[str]:
    base_url = "https://www.research.gov/awardapi-service/v1/awards.json"
    params = {"id": award_id, "printFields": "publicationResearch"}

    try:
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()
        return response.json()

        # TODO: convert Crossref Metadata titles to DOIs
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while fetching data: {e}")
        raise


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


def nih_fetch_award_publication_dois(appl_id: str) -> list[dict]:
    """Fetch the publications associated with an NIH award.

    :param appl_id: the NIH Application ID.
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
            outputs.extend(pubmed_ids_to_dois(pm_ids, "pmid"))
        if len(pmc_ids) > 0:
            outputs.extend(pubmed_ids_to_dois(pmc_ids, "pmcid"))
        return outputs

    except requests.exceptions.RequestException as e:
        logging.error(f"nih_fetch_award_publication_dois: an error occurred while fetching data: {e}")
        raise


def pubmed_ids_to_dois(
    ids: list[int],
    idtype: str,
    versions: str | None = "no",
    tool: str = "dmptool-workflow",
    email: str = EMAIL_ADDRESS,
) -> list[dict]:
    """Call the PubMed ID converter API to convert PubMed IDs and PMC IDs to DOIs: https://pmc.ncbi.nlm.nih.gov/tools/id-converter-api/

    :param ids: a list of PubMed IDs or PMC IDs.
    :param idtype: what type of IDs are supplied: "pmcid", "pmid", "mid", or "doi".
    :param versions: whether to return version information.
    :param tool: the name of the tool.
    :param email: an email address.
    :return:
    """

    batches = get_chunks(input_list=ids, chunk_size=200)
    outputs = []
    for batch in batches:
        outputs.extend(_pubmed_ids_to_dois(batch, idtype, versions, tool, email))
    return outputs


def _pubmed_ids_to_dois(
    ids: list[int], idtype: str, versions: str | None = "no", tool="dmptool-match-workflows", email=EMAIL_ADDRESS
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
