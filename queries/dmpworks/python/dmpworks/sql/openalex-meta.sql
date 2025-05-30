--CREATE OR REPLACE TABLE openalex_meta AS
---- Remove DataCite works from OpenAlex
--SELECT
--  id,
--  doi
--FROM openalex_works
--WHERE doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM datacite_works WHERE openalex_works.doi = datacite_works.doi)

CREATE OR REPLACE TABLE openalex_meta AS
WITH base AS (
  SELECT
    id,
    doi
  FROM openalex_works
  WHERE doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM datacite_works WHERE openalex_works.doi = datacite_works.doi)
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
LEFT JOIN openalex_works oaw ON base.id = oaw.id;