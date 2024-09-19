import argparse
import time

import observatory.platform.bigquery as bq
from academic_observatory_workflows.config import project_path
from observatory.platform.utils.jinja2_utils import render_template


def run_sql_template(template_name: str, dataset_id: str, **context):
    template_path = project_path("cdl_workflow", "sql", f"{template_name}.sql.jinja2")
    sql = render_template(template_path, dataset_id=dataset_id, **context)
    # print(f"template: {template_path}")
    # with open(f"{template_name}.sql", mode="w") as f:
    #     f.write(sql)
    bq.bq_run_query(sql)


def create_embedding_model(*, dataset_id: str, embedding_model_id: str, vertex_ai_model_id: str):
    run_sql_template(
        "embedding_model", dataset_id, embedding_model_id=embedding_model_id, vertex_ai_model_id=vertex_ai_model_id
    )


def normalise_dmps(*, dataset_id: str, ror_table_id: str, dmps_raw_table_id: str, output_table_id: str):
    run_sql_template(
        "normalise_dmps",
        dataset_id,
        ror_table_id=ror_table_id,
        dmps_raw_table_id=dmps_raw_table_id,
        output_table_id=output_table_id,
    )


def normalise_openalex(
    *,
    dataset_id: str,
    openalex_works_table_id: str,
    openalex_funders_table_id: str,
    crossref_metadata_table_id: str,
    datacite_table_id: str,
    dmps_norm_table_id: str,
    output_table_id: str,
):
    run_sql_template(
        "normalise_openalex",
        dataset_id,
        openalex_works_table_id=openalex_works_table_id,
        openalex_funders_table_id=openalex_funders_table_id,
        crossref_metadata_table_id=crossref_metadata_table_id,
        datacite_table_id=datacite_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        output_table_id=output_table_id,
    )


def normalise_crossref(
    *,
    dataset_id: str,
    crossref_metadata_table_id: str,
    ror_table_id: str,
    openalex_norm_table_id: str,
    dmps_norm_table_id: str,
    output_table_id: str,
):
    run_sql_template(
        "normalise_crossref",
        dataset_id,
        crossref_metadata_table_id=crossref_metadata_table_id,
        ror_table_id=ror_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        output_table_id=output_table_id,
    )


def normalise_datacite(
    *,
    dataset_id: str,
    datacite_table_id: str,
    ror_table_id: str,
    dmps_norm_table_id: str,
    openalex_norm_table_id: str,
    output_table_id: str,
):
    run_sql_template(
        "normalise_datacite",
        dataset_id,
        datacite_table_id=datacite_table_id,
        ror_table_id=ror_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        output_table_id=output_table_id,
    )


def match_intermediate(
    *, dataset_id: str, dmps_norm_table_id: str, dataset_table_id: str, output_table_id: str, max_results: int = 100
):
    run_sql_template(
        "match_intermediate",
        dataset_id,
        dmps_norm_table_id=dmps_norm_table_id,
        dataset_table_id=dataset_table_id,
        output_table_id=output_table_id,
        max_results=max_results,
    )


def get_dmps_raw_table(output_project_id: str, dataset_id: str, table_name="dmps_raw"):
    dmps_raw_table_id = bq.bq_table_id(output_project_id, dataset_id, table_name)
    release_date = bq.bq_select_table_shard_dates(dmps_raw_table_id, limit=1)
    if len(release_date) != 1:
        raise ValueError(f"No shards found for: {dmps_raw_table_id}")
    release_date = release_date[0]
    return bq.bq_sharded_table_id(output_project_id, dataset_id, table_name, release_date), release_date


def create_dmps_content_table(
    *,
    dataset_id: str,
    dmps_norm_table_id: str,
    output_table_id: str,
):
    run_sql_template(
        "dmps_content_table",
        dataset_id,
        dmps_norm_table_id=dmps_norm_table_id,
        output_table_id=output_table_id,
    )


def create_content_table(
    *,
    dataset_id: str,
    dataset_table_id: str,
    match_intermediate_table_id: str,
    output_table_id: str,
):
    run_sql_template(
        "match_content_table",
        dataset_id,
        dataset_table_id=dataset_table_id,
        match_intermediate_table_id=match_intermediate_table_id,
        output_table_id=output_table_id,
    )


def generate_embeddings(*, dataset_id: str, content_table_id: str, embedding_model_id: str, output_table_id: str):
    run_sql_template(
        "generate_embeddings",
        dataset_id,
        content_table_id=content_table_id,
        embedding_model_id=embedding_model_id,
        output_table_id=output_table_id,
    )


def match_vector_search(
    *,
    dataset_id: str,
    match_intermediate_table_id: str,
    dataset_table_id: str,
    dmps_norm_table_id: str,
    dataset_embeddings_table_id: str,
    dmps_embeddings_table_id: str,
    output_table_id: str,
):
    run_sql_template(
        "match_vector_search",
        dataset_id,
        match_intermediate_table_id=match_intermediate_table_id,
        dataset_table_id=dataset_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        dataset_embeddings_table_id=dataset_embeddings_table_id,
        dmps_embeddings_table_id=dmps_embeddings_table_id,
        output_table_id=output_table_id,
    )


def create_dmp_matches(*, input_project_id: str, output_project_id: str, dataset_id: str, vertex_ai_model_id: str):
    sources = ["openalex", "crossref", "datacite"]

    # Input tables
    # fmt: off
    dmps_raw_table_id, release_date = get_dmps_raw_table(output_project_id, dataset_id)
    ror_table_id = bq.bq_select_latest_table(bq.bq_table_id(input_project_id, "ror", "ror"))
    openalex_works_table_id = bq.bq_table_id(input_project_id, "openalex", "works")
    openalex_funders_table_id = bq.bq_table_id(input_project_id, "openalex", "funders")
    crossref_metadata_table_id = bq.bq_select_latest_table(bq.bq_table_id(input_project_id, "crossref_metadata", "crossref_metadata"))
    datacite_table_id = bq.bq_select_latest_table(bq.bq_table_id(input_project_id, "datacite", "datacite"))
    # fmt: on

    # Normalised tables
    dmps_norm_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, "dmps", release_date)
    openalex_norm_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, "openalex", release_date)
    crossref_norm_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, "crossref", release_date)
    datacite_norm_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, "datacite", release_date)

    # Create shared functions and embedding model
    embedding_model_id = f"{dataset_id}.embedding_model"
    run_sql_template("shared_functions", dataset_id)
    create_embedding_model(
        dataset_id=dataset_id, embedding_model_id=embedding_model_id, vertex_ai_model_id=vertex_ai_model_id
    )

    # Normalise datasets
    # fmt: off
    normalise_dmps(
        dataset_id=dataset_id,
        ror_table_id=ror_table_id,
        dmps_raw_table_id=dmps_raw_table_id,
        output_table_id=dmps_norm_table_id
    )
    normalise_openalex(
        dataset_id=dataset_id,
        openalex_works_table_id=openalex_works_table_id,
        openalex_funders_table_id=openalex_funders_table_id,
        crossref_metadata_table_id=crossref_metadata_table_id,
        datacite_table_id=datacite_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        output_table_id=openalex_norm_table_id,
    )
    normalise_crossref(
        dataset_id=dataset_id,
        crossref_metadata_table_id=crossref_metadata_table_id,
        ror_table_id=ror_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        output_table_id=crossref_norm_table_id,
    )
    normalise_datacite(
        dataset_id=dataset_id,
        datacite_table_id=datacite_table_id,
        ror_table_id=ror_table_id,
        dmps_norm_table_id=dmps_norm_table_id,
        openalex_norm_table_id=openalex_norm_table_id,
        output_table_id=datacite_norm_table_id,
    )
    # fmt: on

    # Generate intermediate matches with the DMP table
    for source in sources:
        dataset_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, source, release_date)
        match_intermediate_table_id = bq.bq_sharded_table_id(
            output_project_id, dataset_id, f"{source}_match_intermediate", release_date
        )
        match_intermediate(
            dataset_id=dataset_id,
            dmps_norm_table_id=dmps_norm_table_id,
            dataset_table_id=dataset_table_id,
            output_table_id=match_intermediate_table_id,
            max_results=100,
        )

    # Generate content tables
    dmps_content_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, "dmps_content", release_date)
    create_dmps_content_table(
        dataset_id=dataset_id, dmps_norm_table_id=dmps_norm_table_id, output_table_id=dmps_content_table_id
    )
    for source in sources:
        dataset_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, source, release_date)
        match_intermediate_table_id = bq.bq_sharded_table_id(
            output_project_id, dataset_id, f"{source}_match_intermediate", release_date
        )
        output_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, f"{source}_content", release_date)
        create_content_table(
            dataset_id=dataset_id,
            dataset_table_id=dataset_table_id,
            match_intermediate_table_id=match_intermediate_table_id,
            output_table_id=output_table_id,
        )

    # Generate embeddings for intermediate matches
    for source in sources + ["dmps"]:
        content_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, f"{source}_content", release_date)
        output_table_id = bq.bq_sharded_table_id(
            output_project_id, dataset_id, f"{source}_content_embeddings", release_date
        )
        generate_embeddings(
            dataset_id=dataset_id,
            content_table_id=content_table_id,
            embedding_model_id=embedding_model_id,
            output_table_id=output_table_id,
        )

    # Add vector search to matches
    for source in sources:
        dataset_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, source, release_date)
        match_intermediate_table_id = bq.bq_sharded_table_id(
            output_project_id, dataset_id, f"{source}_match_intermediate", release_date
        )
        dmps_embeddings_table_id = bq.bq_sharded_table_id(
            output_project_id, dataset_id, "dmps_content_embeddings", release_date
        )
        dataset_embeddings_table_id = bq.bq_sharded_table_id(
            output_project_id, dataset_id, f"{source}_content_embeddings", release_date
        )
        output_table_id = bq.bq_sharded_table_id(output_project_id, dataset_id, f"{source}_match", release_date)
        match_vector_search(
            dataset_id=dataset_id,
            match_intermediate_table_id=match_intermediate_table_id,
            dataset_table_id=dataset_table_id,
            dmps_norm_table_id=dmps_norm_table_id,
            dataset_embeddings_table_id=dataset_embeddings_table_id,
            dmps_embeddings_table_id=dmps_embeddings_table_id,
            output_table_id=output_table_id,
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Match works from OpenAlex, Crossref and DataCite to DMPTool DMPs")

    # Required arguments
    parser.add_argument("input_project_id", type=str, help="ID of the input Google Cloud project")
    parser.add_argument("output_project_id", type=str, help="ID of the output Google Cloud project")

    # Optional argument with a default value
    parser.add_argument("--dataset_id", type=str, default="cdl_dmps", help="BigQuery dataset ID where results should be saved")
    parser.add_argument(
        "--vertex_ai_model_id",
        type=str,
        default="text-multilingual-embedding-002",
        help="The identifier for the Vertex AI model to use for generating embeddings",
    )

    return parser.parse_args()


if __name__ == "__main__":
    start_time = time.time()
    args = parse_args()
    create_dmp_matches(
        input_project_id=args.input_project_id,
        output_project_id=args.output_project_id,
        dataset_id=args.dataset_id,
        vertex_ai_model_id=args.vertex_ai_model_id,
    )
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")
