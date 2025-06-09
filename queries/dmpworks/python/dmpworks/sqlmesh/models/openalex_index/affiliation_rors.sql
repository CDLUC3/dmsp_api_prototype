MODEL (
  name openalex_index.affiliation_rors,
  dialect duckdb,
  kind FULL
);

SELECT
  owm.doi,
  ARRAY_AGG_DISTINCT(ror) AS affiliation_rors,
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works_affiliations ON owm.id = work_id
WHERE ror IS NOT NULL
GROUP BY owm.doi;