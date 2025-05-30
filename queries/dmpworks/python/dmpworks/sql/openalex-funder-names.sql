CREATE OR REPLACE TABLE openalex_funder_names AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(funder_name) AS funder_names
FROM (
  -- OpenAlex
  SELECT idx.id, idx.doi, funder_display_name AS funder_name
  FROM openalex_meta AS idx
  INNER JOIN openalex_works_funders owf ON idx.id = owf.work_id
  WHERE funder_display_name IS NOT NULL

  UNION ALL

  -- Crossref Metadata
  SELECT idx.id, idx.doi, name AS funder_name
  FROM openalex_meta AS idx
  INNER JOIN crossref_works_funders cwf ON idx.doi = cwf.work_doi
  WHERE name IS NOT NULL
)
GROUP BY doi