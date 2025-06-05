CREATE OR REPLACE TABLE datacite_affiliation_names AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(name) AS affiliation_names,
FROM (
  -- DataCite
  SELECT doi, name
  FROM datacite_works dw
  INNER JOIN datacite_works_affiliations dwa ON dw.doi = dwa.work_doi
  WHERE name IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, display_name AS name
  FROM datacite_works dw
  INNER JOIN openalex_works_affiliations owa ON dw.doi = owa.work_doi
  WHERE display_name IS NOT NULL
)
GROUP BY doi