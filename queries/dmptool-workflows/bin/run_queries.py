import argparse

import pendulum
from pendulum.exceptions import ParserError

from dmptool_workflows.dmp_match_workflow.tasks import create_dmp_matches


def valid_date(date_str):
    try:
        return pendulum.parse(date_str).date()
    except ParserError:
        raise argparse.ArgumentTypeError(f"Not a valid date: '{date_str}'. Expected format: YYYY-MM-DD.")


def parse_args():
    parser = argparse.ArgumentParser(description="Process input and output project IDs, and dataset ID.")

    # Required arguments
    parser.add_argument("ao_project_id", type=str, help="The Academic Observatory Google Cloud project ID")
    parser.add_argument("dmps_project_id", type=str, help="The DMPs Google Cloud project ID")
    parser.add_argument("release_date", type=valid_date, help="The date for sharding the results.")

    # Optional argument with a default value
    parser.add_argument(
        "--dataset_id", type=str, default="cdl_dmps_test", help="The BigQuery dataset ID where results should be stored"
    )
    parser.add_argument(
        "--vertex_ai_model_id", type=str, default="text-multilingual-embedding-002", help="The Vertex AI model ID"
    )
    parser.add_argument(
        "--weighted_count_threshold",
        type=int,
        default=3,
        help="The threshold to pre-filter intermediate matches before running embeddings and vector search",
    )
    parser.add_argument(
        "--max_matches", type=int, default=100, help="The maximum number of matches to return for each DMP"
    )
    parser.add_argument("--dry_run", action="store_true", help="Whether to do a dry run of the queries")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    create_dmp_matches(
        ao_project_id=args.ao_project_id,
        dmps_project_id=args.dmps_project_id,
        dataset_id=args.dataset_id,
        release_date=args.release_date,
        vertex_ai_model_id=args.vertex_ai_model_id,
        weighted_count_threshold=args.weighted_count_threshold,
        max_matches=args.max_matches,
        dry_run=args.dry_run,
    )
