CREATE OR REPLACE TABLE openalex_funder_ids AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(funder_id) AS funder_ids
FROM (
  -- OpenAlex
  -- Get OpenAlex funder Crossref Funder IDs
  SELECT idx.id, idx.doi, ids.doi AS funder_id
  FROM openalex_meta AS idx
  INNER JOIN openalex_works_funders owf ON idx.id = owf.work_id
  LEFT JOIN openalex_funders oaf ON owf.funder_id = oaf.id
  WHERE ids.doi IS NOT NULL

  UNION ALL

  -- Get ROR IDs
  -- Get OpenAlex funder ROR IDs
  SELECT idx.id, idx.doi, ids.ror AS funder_id
  FROM openalex_meta AS idx
  INNER JOIN openalex_works_funders owf ON idx.id = owf.work_id
  LEFT JOIN openalex_funders oaf ON owf.funder_id = oaf.id
  WHERE ids.ror IS NOT NULL

  UNION ALL

  -- Crossref Metadata
  SELECT idx.id, idx.doi, funder_doi AS funder_id
  FROM openalex_meta AS idx
  INNER JOIN crossref_works_funders cwf ON idx.doi = cwf.work_doi
  WHERE funder_doi IS NOT NULL
)
GROUP BY doi