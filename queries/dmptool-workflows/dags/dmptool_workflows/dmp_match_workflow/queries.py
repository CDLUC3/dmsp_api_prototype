from typing import Optional

import observatory_platform.google.bigquery as bq
from google.cloud import bigquery
from observatory_platform.jinja2_utils import render_template

from dmptool_workflows.config import project_path


def run_sql_template(
    template_name: str,
    dataset_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
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
        bq.bq_run_query(sql, client=bq_client)


def create_embedding_model(
    *,
    dataset_id: str,
    embedding_model_id: str,
    vertex_ai_model_id: str,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
):
    run_sql_template(
        "embedding_model",
        dataset_id,
        dry_run=dry_run,
        embedding_model_id=embedding_model_id,
        vertex_ai_model_id=vertex_ai_model_id,
        bq_client=bq_client,
    )


def normalise_dmps(
    *,
    dataset_id: str,
    ror_table_id: str,
    dmps_raw_table_id: str,
    dmps_norm_table_id: str,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
):
    run_sql_template(
        "normalise_dmps",
        dataset_id,
        dry_run=dry_run,
        ror_table_id=ror_table_id,
        dmps_raw_table_id=dmps_raw_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        bq_client=bq_client,
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
):
    run_sql_template(
        "normalise_openalex",
        dataset_id,
        dry_run=dry_run,
        openalex_works_table_id=openalex_works_table_id,
        openalex_funders_table_id=openalex_funders_table_id,
        crossref_metadata_table_id=crossref_metadata_table_id,
        datacite_table_id=datacite_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        bq_client=bq_client,
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
):
    run_sql_template(
        "normalise_crossref",
        dataset_id,
        dry_run=dry_run,
        crossref_metadata_table_id=crossref_metadata_table_id,
        ror_table_id=ror_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        crossref_norm_table_id=crossref_norm_table_id,
        bq_client=bq_client,
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
):
    run_sql_template(
        "normalise_datacite",
        dataset_id,
        dry_run=dry_run,
        datacite_table_id=datacite_table_id,
        ror_table_id=ror_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        datacite_norm_table_id=datacite_norm_table_id,
        bq_client=bq_client,
    )


def match_intermediate(
    *,
    dataset_id: str,
    dmps_norm_table_id: str,
    match_norm_table_id: str,
    match_intermediate_table_id: str,
    weighted_count_threshold: int = 3,
    max_matches: int = 100,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
):
    run_sql_template(
        "match_intermediate",
        dataset_id,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        dmps_norm_table_id=dmps_norm_table_id,
        match_norm_table_id=match_norm_table_id,
        match_intermediate_table_id=match_intermediate_table_id,
        weighted_count_threshold=weighted_count_threshold,
        max_matches=max_matches,
        bq_client=bq_client,
    )


def create_dmps_content_table(
    *,
    dataset_id: str,
    dmps_norm_table_id: str,
    dmps_content_table_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
):
    run_sql_template(
        "dmps_content_table",
        dataset_id,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        dmps_norm_table_id=dmps_norm_table_id,
        dmps_content_table_id=dmps_content_table_id,
        bq_client=bq_client,
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
):
    run_sql_template(
        "update_content_table",
        dataset_id,
        dataset_name=dataset_name,
        content_table_id=content_table_id,
        embeddings_table_id=embeddings_table_id,
        norm_table_id=norm_table_id,
        match_intermediate_table_id=match_intermediate_table_id,
        bq_client=bq_client,
        dry_run = dry_run,
        dry_run_id = dry_run_id,
    )


def update_embeddings(
    *,
    dataset_id: str,
    content_table_id: str,
    embedding_model_id: str,
    embeddings_table_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
):
    run_sql_template(
        "update_embeddings",
        dataset_id,
        dry_run=dry_run,
        dry_run_id=dry_run_id,
        content_table_id=content_table_id,
        embedding_model_id=embedding_model_id,
        embeddings_table_id=embeddings_table_id,
        bq_client=bq_client,
    )


def match_vector_search(
    *,
    dataset_id: str,
    match_intermediate_table_id: str,
    match_norm_table_id: str,
    dmps_norm_table_id: str,
    match_embeddings_table_id: str,
    dmps_embeddings_table_id: str,
    match_table_id: str,
    dry_run: bool = False,
    dry_run_id: Optional[str] = None,
    bq_client: bigquery.Client = None,
):
    run_sql_template(
        "match_vector_search",
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
    )
