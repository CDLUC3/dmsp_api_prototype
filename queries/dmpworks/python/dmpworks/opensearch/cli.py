import logging
import pathlib
from typing import Annotated, Optional

from cyclopts import App, Parameter, validators

from dmpworks.cli_utils import Directory, LogLevel
from dmpworks.opensearch.dmp_works import dmp_works_search
from dmpworks.opensearch.enrich_dmps import enrich_dmps
from dmpworks.opensearch.index import create_index, update_mapping
from dmpworks.opensearch.sync_dmps import sync_dmps
from dmpworks.opensearch.sync_works import sync_works
from dmpworks.opensearch.utils import (
    Date,
    make_opensearch_client,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
)

app = App(name="opensearch", help="OpenSearch utilities.")


@app.command(name="create-index")
def create_index_cmd(
    index_name: str,
    mapping_filename: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Create an OpenSearch index.

    Args:
        index_name: The name of the OpenSearch index to create (e.g., works).
        mapping_filename: The name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        client_config: OpenSearch client settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(client_config)
    create_index(client, index_name, mapping_filename)


@app.command(name="update-mapping")
def update_mapping_cmd(
    index_name: str,
    mapping_filename: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Update an OpenSearch index mapping.

    Args:
        index_name: The name of the OpenSearch index to update (e.g., works).
        mapping_filename: The name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        client_config: OpenSearch client settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(client_config)
    update_mapping(client, index_name, mapping_filename)


@app.command(name="sync-works")
def sync_works_cmd(
    index_name: str,
    in_dir: Directory,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool Works Index Table with OpenSearch.

    Args:
        index_name: The name of the OpenSearch index to sync to (e.g., works).
        in_dir: Path to the DMP Tool Works index table export directory (e.g., /path/to/export).
        client_config: OpenSearch client settings.
        sync_config: OpenSearch sync settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    if sync_config is None:
        sync_config = OpenSearchSyncConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    sync_works(
        index_name,
        in_dir,
        client_config,
        sync_config,
        log_level=level,
    )


@app.command(name="sync-dmps")
def sync_dmps_cmd(
    index_name: str,
    in_dir: Directory,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool DMP Table with OpenSearch.

    Args:
        index_name: The name of the OpenSearch index to sync to (e.g., dmps).
        in_dir: Path to the DMP Tool DMPs export directory (e.g., /path/to/export).
        client_config: OpenSearch client settings.
        sync_config: OpenSearch sync settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    if sync_config is None:
        sync_config = OpenSearchSyncConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    sync_dmps(
        index_name,
        in_dir,
        client_config,
        sync_config,
        log_level=level,
    )


@app.command(name="enrich-dmps")
def enrich_dmps_cmd(
    dmp_index_name: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Enrich DMPs in OpenSearch, including fetching publications that can be
    found on funder award pages.

    Args:
        dmp_index_name: the name of the DMP index to update.
        client_config: OpenSearch client settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    enrich_dmps(
        dmp_index_name,
        client_config,
    )


@app.command(name="dmp-works-search")
def dmp_works_search_cmd(
    dmp_index_name: str,
    works_index_name: str,
    out_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=False,
                exists=False,
            )
        ),
    ],
    scroll_time: str = "60m",
    batch_size: int = 250,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    parallel_search: bool = False,
    include_named_queries_score: bool = True,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
    client_config: Optional[OpenSearchClientConfig] = None,
    dmp_inst_name: Optional[str] = None,
    dmp_inst_ror: Optional[str] = None,
    start_date: Date = None,
    end_date: Date = None,
    log_level: LogLevel = "INFO",
):
    """Enrich DMPs in OpenSearch, including fetching publications that can be
    found on funder award pages.

    Args:
        dmp_index_name: the name of the DMP index in OpenSearch.
        works_index_name: the name of the works index in OpenSearch.
        out_file: the output directory where search results will be saved.
        scroll_time: the length of time the OpenSearch scroll used to iterate
        through DMPs will stay active. Set it to a value greater than the length
        of this process.
        batch_size: the number of searches run in parallel when include_scores=False.
        max_results: the maximum number of matches per DMP.
        project_end_buffer_years: the number of years to add to the end of the
        project end date when searching for works.
        parallel_search: whether to run parallel search or not.
        include_named_queries_score: whether to include scores for subqueries.
        max_concurrent_searches: the maximum number of concurrent searches.
        max_concurrent_shard_requests: the maximum number of shards searched per node.
        client_config: OpenSearch client settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    dmp_works_search(
        dmp_index_name,
        works_index_name,
        out_file,
        client_config,
        scroll_time=scroll_time,
        batch_size=batch_size,
        max_results=max_results,
        project_end_buffer_years=project_end_buffer_years,
        parallel_search=parallel_search,
        include_named_queries_score=include_named_queries_score,
        max_concurrent_searches=max_concurrent_searches,
        max_concurrent_shard_requests=max_concurrent_shard_requests,
        dmp_inst_name=dmp_inst_name,
        dmp_inst_ror=dmp_inst_ror,
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    app()
