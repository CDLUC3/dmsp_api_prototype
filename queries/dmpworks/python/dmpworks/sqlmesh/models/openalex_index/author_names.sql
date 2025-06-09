MODEL (
  name openalex_index.author_names,
  dialect duckdb,
  kind FULL
);

SELECT
  owm.doi,
  ARRAY_AGG_DISTINCT(display_name) AS author_names,
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works_authors ON owm.id = work_id
WHERE display_name IS NOT NULL
GROUP BY owm.doi;