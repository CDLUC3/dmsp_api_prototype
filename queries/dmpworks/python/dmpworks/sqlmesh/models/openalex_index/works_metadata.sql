MODEL (
  name openalex_index.works_metadata,
  dialect duckdb,
  kind FULL
);

WITH base AS (
  SELECT
    id,
    doi
  FROM openalex.works
  WHERE doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM datacite_works WHERE openalex.works.doi = datacite_works.doi)
),
counts AS (
  SELECT
    doi,
    COUNT(*) AS doi_count
  FROM base
  GROUP BY doi
)
SELECT
  base.id,
  base.doi,
  LENGTH(oaw.title) AS title_length,
  LENGTH(oaw.abstract) AS abstract_length,
  counts.doi_count > 1 AS is_duplicate
FROM base
LEFT JOIN counts ON base.doi = counts.doi
LEFT JOIN openalex.works oaw ON base.id = oaw.id;