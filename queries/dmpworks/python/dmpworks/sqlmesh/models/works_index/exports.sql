/*
  works_index.exports:

  Exports the works index to Parquet files to the export_path specified in
  config.yaml. Updates the works_index.exports with the export date.
*/

MODEL (
  name works_index.exports,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key export_date
  ),
  columns (
    export_date TIMESTAMP
  )
);

-- Record export date
SELECT @end_ds AS export_date;

-- Export data
COPY (
  SELECT
    *,
    'datacite' AS source
  FROM datacite_index

  UNION ALL

  SELECT
    *,
    'openalex' AS source
  FROM openalex_index
) TO @VAR('export_path') (FORMAT PARQUET, FILE_SIZE_BYTES '100MB');