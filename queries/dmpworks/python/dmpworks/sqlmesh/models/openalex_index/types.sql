MODEL (
  name openalex_index.types,
  dialect duckdb,
  kind FULL
);

SELECT
  owm.doi,
  MIN(type) AS type, -- Could change to most common
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works ow ON owm.id = ow.id
WHERE type IS NOT NULL
GROUP BY owm.doi;