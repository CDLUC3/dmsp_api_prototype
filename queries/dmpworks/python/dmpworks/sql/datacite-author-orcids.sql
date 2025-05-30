CREATE OR REPLACE TABLE datacite_author_orcids AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(orcid) AS author_orcids,
  FROM (
  -- Only use ORCID IDs for author identifiers

  -- DataCite
  SELECT doi, orcid
  FROM datacite_works dw
  INNER JOIN datacite_works_authors dwa ON dw.doi = dwa.work_doi
  WHERE orcid IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, orcid
  FROM datacite_works dw
  INNER JOIN openalex_works_authors owa ON dw.doi = owa.work_doi
  WHERE orcid IS NOT NULL
)
GROUP BY doi