CREATE OR REPLACE TABLE datacite_funder_names AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(name) AS funder_names,
FROM (
  -- DataCite
  SELECT doi, funder_name AS name
  FROM datacite_works dw
  INNER JOIN datacite_works_funders dwf ON dw.doi = dwf.work_doi
  WHERE funder_name IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, funder_display_name AS name
  FROM datacite_works dw
  INNER JOIN openalex_works_funders owf ON dw.doi = owf.work_doi
  WHERE funder_display_name IS NOT NULL
)
GROUP BY doi