CREATE OR REPLACE TABLE datacite_updated_dates AS
-- Choose the most recent updated date from DataCite and OpenAlex
SELECT
  doi,
  MAX(updated_date) AS updated_date
FROM (
  SELECT doi, updated_date
  FROM datacite_works
  WHERE updated_date IS NOT NULL

  UNION ALL

  SELECT doi, updated_date
  FROM openalex_works
  WHERE doi IS NOT NULL AND updated_date IS NOT NULL
)
GROUP BY doi