CREATE OR REPLACE TABLE datacite_affiliation_rors AS
SELECT
  doi,
  ARRAY_AGG_DISTINCT(ror) AS affiliation_rors,
FROM (
  -- Only use ROR IDs for affiliation IDs

  -- DataCite
  -- Convert various identifiers (ROR, GRID, ISNI) to ROR
  SELECT doi, ror_index.identifier AS ror
  FROM datacite_works dw
  INNER JOIN datacite_works_affiliations dwa ON dw.doi = dwa.work_doi
  INNER JOIN ror_index ON dwa.affiliation_identifier = ror_index.identifier

  UNION ALL

  -- OpenAlex
  SELECT doi, ror
  FROM datacite_works dw
  INNER JOIN openalex_works_affiliations owa ON dw.doi = owa.work_doi
  WHERE ror IS NOT NULL
)
GROUP BY doi