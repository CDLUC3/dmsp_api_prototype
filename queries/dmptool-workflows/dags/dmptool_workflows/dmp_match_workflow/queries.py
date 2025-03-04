from typing import Dict, List, Optional

from google.cloud import bigquery

import observatory_platform.google.bigquery as bq
from dmptool_workflows.config import project_path
from dmptool_workflows.dmp_match_workflow.tasks import DMP
from dmptool_workflows.dmp_match_workflow.types import Fund, Funder
from observatory_platform.jinja2_utils import render_template


def run_sql_template(
    template_name: str,
    dataset_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
    **context,
):

    template_path = project_path("dmp_match_workflow", "sql", f"{template_name}.sql.jinja2")
    sql = render_template(template_path, dataset_id=dataset_id, **context)
    print(sql)
    if dry_run:
        print(f"dry run query from template: {template_path}")
        file_name = f"{template_name}_{dry_run_id}.sql" if dry_run_id is not None else f"{template_name}.sql"
        with open(file_name, mode="w") as f:
            f.write(sql)
    else:
        print(f"running query from template: {template_path}")
        job_config = None
        if bq_query_labels is not None:
            job_config = bigquery.QueryJobConfig(labels=bq_query_labels)
        bq.bq_run_query(sql, client=bq_client, job_config=job_config)


def merge_labels(labels_a: dict | None, labels_b: dict | None):
    if labels_a is None:
        labels_a = {}
    if labels_b is None:
        labels_b = {}
    return {**labels_a, **labels_b}


def create_embedding_model(
    *,
    dataset_id: str,
    embedding_model_id: str,
    vertex_ai_model_id: str,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "embedding_model"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        embedding_model_id=embedding_model_id,
        vertex_ai_model_id=vertex_ai_model_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"query_id": template_name}),
    )


def get_dmps_funding(
    *,
    dmps_raw_table_id: str,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
) -> List[DMP]:
    job_config = None
    if bq_query_labels is not None:
        job_config = bigquery.QueryJobConfig(labels=bq_query_labels)

    # Select data
    rows = bq.bq_run_query(
        f"SELECT dmp_id, funding FROM `{dmps_raw_table_id}`", client=bq_client, job_config=job_config
    )

    # Convert to list of DMPs
    dmps = parse_dmps(rows)

    return dmps


def parse_dmps(dmps_raw: List[Dict]) -> List[DMP]:
    """Parse the raw DMPs table into DMP objects.

    :param dmps_raw: the raw DMPs table.
    :return: a list of DMP objects.
    """

    dmps = []
    for dmp in dmps_raw:
        dmp_id = dmp.get("dmp_id")
        funding = []
        for fund in dmp.get("funding", []):
            funder = fund.get("funder", {})
            funder_id = funder.get("id")
            funder_name = funder.get("name")
            funding_opportunity_id = fund.get("funding_opportunity_id")
            grant_id = fund.get("grant_id")
            funding.append(
                Fund(
                    funder=Funder(id=funder_id, name=funder_name),
                    funding_opportunity_id=funding_opportunity_id,
                    grant_id=grant_id,
                )
            )
        dmps.append(DMP(dmp_id=dmp_id, funding=funding))

    return dmps


def normalise_dmps(
    *,
    dataset_id: str,
    ror_table_id: str,
    dmps_raw_table_id: str,
    dmps_norm_table_id: str,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "normalise_dmps"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        ror_table_id=ror_table_id,
        dmps_raw_table_id=dmps_raw_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": "dmps", "query_id": template_name}),
    )


def normalise_openalex(
    *,
    dataset_id: str,
    openalex_works_table_id: str,
    openalex_funders_table_id: str,
    crossref_metadata_table_id: str,
    datacite_table_id: str,
    dmps_norm_table_id: str,
    openalex_norm_table_id: str,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "normalise_openalex"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        openalex_works_table_id=openalex_works_table_id,
        openalex_funders_table_id=openalex_funders_table_id,
        crossref_metadata_table_id=crossref_metadata_table_id,
        datacite_table_id=datacite_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": "openalex", "query_id": template_name}),
    )


def normalise_crossref(
    *,
    dataset_id: str,
    crossref_metadata_table_id: str,
    ror_table_id: str,
    openalex_norm_table_id: str,
    dmps_norm_table_id: str,
    crossref_norm_table_id: str,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "normalise_crossref"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        crossref_metadata_table_id=crossref_metadata_table_id,
        ror_table_id=ror_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        crossref_norm_table_id=crossref_norm_table_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": "crossref_metadata", "query_id": template_name}),
    )


def normalise_datacite(
    *,
    dataset_id: str,
    datacite_table_id: str,
    ror_table_id: str,
    dmps_norm_table_id: str,
    openalex_norm_table_id: str,
    datacite_norm_table_id: str,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "normalise_datacite"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        datacite_table_id=datacite_table_id,
        ror_table_id=ror_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        datacite_norm_table_id=datacite_norm_table_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": "datacite", "query_id": template_name}),
    )


def match_intermediate(
    *,
    dataset_id: str,
    dataset_name: str,
    dmps_norm_table_id: str,
    match_norm_table_id: str,
    match_intermediate_table_id: str,
    weighted_count_threshold: int = 3,
    max_matches: int = 100,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "match_intermediate"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        dmps_norm_table_id=dmps_norm_table_id,
        match_norm_table_id=match_norm_table_id,
        match_intermediate_table_id=match_intermediate_table_id,
        weighted_count_threshold=weighted_count_threshold,
        max_matches=max_matches,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": dataset_name, "query_id": template_name}),
    )


def create_dmps_content_table(
    *,
    dataset_id: str,
    dmps_norm_table_id: str,
    dmps_content_table_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "dmps_content_table"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        dmps_norm_table_id=dmps_norm_table_id,
        dmps_content_table_id=dmps_content_table_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": "dmps", "query_id": template_name}),
    )


def update_content_table(
    *,
    dataset_id: str,
    dataset_name: str,
    content_table_id: str,
    embeddings_table_id: str,
    norm_table_id: str,
    match_intermediate_table_id: Optional[str],  # Not used for generating content for DMP dataset
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "update_content_table"
    run_sql_template(
        template_name,
        dataset_id,
        dataset_name=dataset_name,
        content_table_id=content_table_id,
        embeddings_table_id=embeddings_table_id,
        norm_table_id=norm_table_id,
        match_intermediate_table_id=match_intermediate_table_id,
        bq_client=bq_client,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": dataset_name, "query_id": template_name}),
    )


def update_embeddings(
    *,
    dataset_id: str,
    dataset_name: str,
    content_table_id: str,
    embedding_model_id: str,
    embeddings_table_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "update_embeddings"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        content_table_id=content_table_id,
        embedding_model_id=embedding_model_id,
        embeddings_table_id=embeddings_table_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": dataset_name, "query_id": template_name}),
    )


def match_vector_search(
    *,
    dataset_id: str,
    dataset_name: str,
    match_intermediate_table_id: str,
    match_norm_table_id: str,
    dmps_norm_table_id: str,
    match_embeddings_table_id: str,
    dmps_embeddings_table_id: str,
    match_table_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
    bq_query_labels: dict = None,
):
    template_name = "match_vector_search"
    run_sql_template(
        template_name,
        dataset_id,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        match_intermediate_table_id=match_intermediate_table_id,
        match_norm_table_id=match_norm_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        match_embeddings_table_id=match_embeddings_table_id,
        dmps_embeddings_table_id=dmps_embeddings_table_id,
        match_table_id=match_table_id,
        bq_client=bq_client,
        bq_query_labels=merge_labels(bq_query_labels, {"dataset": dataset_name, "query_id": template_name}),
    )
