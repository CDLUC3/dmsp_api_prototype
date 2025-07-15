MODEL (
  name ror.index,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || '/ror/' || @VAR('ror_release_date') || '/transform/parquets/ror.parquet');


