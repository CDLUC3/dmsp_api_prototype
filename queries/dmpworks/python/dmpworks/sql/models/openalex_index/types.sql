/*
  openalex_index.types:

  Chooses the most common type for each DOI from OpenAlex when there are
  multiple OpenAlex works with the same DOI.
*/

MODEL (
  name openalex_index.types,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  owm.doi,
  MODE(type ORDER BY type ASC) AS type,
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works ow ON owm.id = ow.id
WHERE type IS NOT NULL
GROUP BY owm.doi;