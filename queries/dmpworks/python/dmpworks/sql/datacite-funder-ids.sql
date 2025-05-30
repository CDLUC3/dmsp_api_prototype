CREATE OR REPLACE TABLE datacite_funder_ids AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(id) AS funder_ids,
FROM (
  -- Include ROR and Crossref Funder IDs for funder identifiers

  -- DataCite
  -- Include DataCite funder Crossref Funder IDs
  SELECT doi, funder_identifier AS id
  FROM datacite_works dw
  INNER JOIN datacite_works_funders dwf ON dw.doi = dwf.work_doi
  WHERE funder_identifier_type = 'Crossref Funder ID' AND funder_identifier IS NOT NULL

  UNION ALL

  -- Convert DataCite funder IDs to ROR IDs
  SELECT doi, ror_index.ror_id AS id
  FROM datacite_works dw
  INNER JOIN datacite_works_funders dwf ON dw.doi = dwf.work_doi
  INNER JOIN ror_index ON dwf.funder_identifier = ror_index.identifier
  WHERE ror_index.ror_id IS NOT NULL

  UNION ALL

  -- OpenAlex
  -- Get OpenAlex funder Crossref Funder IDs
  SELECT doi, ids.doi AS id
  FROM datacite_works dw
  INNER JOIN openalex_works_funders owf ON dw.doi = owf.work_doi
  INNER JOIN openalex_funders oaf ON owf.funder_id = oaf.id
  WHERE ids.doi IS NOT NULL

  UNION ALL

  -- Get ROR IDs
  -- Get OpenAlex funder ROR IDs
  SELECT doi, ids.ror AS id
  FROM datacite_works dw
  INNER JOIN openalex_works_funders owf ON dw.doi = owf.work_doi
  INNER JOIN openalex_funders oaf ON owf.funder_id = oaf.id
  WHERE ids.ror IS NOT NULL
)
GROUP BY doi