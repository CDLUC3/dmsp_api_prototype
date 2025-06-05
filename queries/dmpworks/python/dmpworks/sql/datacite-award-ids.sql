CREATE OR REPLACE TABLE datacite_award_ids AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(award_id) AS award_ids,
FROM (
  -- DataCite
  SELECT doi, award_number AS award_id
  FROM datacite_works dw
  INNER JOIN datacite_works_funders dwf ON dw.doi = dwf.work_doi
  WHERE award_number IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, award_id
  FROM datacite_works dw
  INNER JOIN openalex_works_funders owf ON dw.doi = owf.work_doi
  WHERE award_id IS NOT NULL
)
GROUP BY doi