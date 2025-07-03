MODEL (
  name crossref_index.works_metadata,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  LENGTH(title) AS title_length,
  LENGTH(abstract) AS abstract_length,
FROM crossref_metadata.works;