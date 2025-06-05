CREATE OR REPLACE TABLE openalex_award_ids AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(award_id) AS award_ids
FROM (
  -- OpenAlex
  SELECT idx.id, idx.doi, award_id
  FROM openalex_meta AS idx
  INNER JOIN openalex_works_funders ON idx.id = work_id
  WHERE award_id IS NOT NULL

  UNION ALL

  -- Crossref Metadata
  SELECT idx.id, idx.doi, award AS award_id
  FROM openalex_meta AS idx
  INNER JOIN crossref_works_funders ON idx.doi = work_doi
  WHERE award IS NOT NULL
)
GROUP BY doi