CREATE OR REPLACE TABLE openalex_updated_dates AS
-- Choose most recent update_date from OpenAlex and Crossref Metadata
SELECT
  doi,
  MAX(updated_date) AS updated_date
FROM (
  SELECT idx.doi, updated_date
  FROM openalex_meta AS idx
  INNER JOIN openalex_works ow ON idx.id = ow.id
  WHERE updated_date IS NOT NULL

  UNION ALL

  SELECT doi, updated_date
  FROM crossref_works
  WHERE doi IS NOT NULL AND updated_date IS NOT NULL
)
GROUP BY doi