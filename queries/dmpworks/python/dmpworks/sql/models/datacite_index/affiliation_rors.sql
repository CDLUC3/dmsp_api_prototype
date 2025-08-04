/*
  datacite_index.affiliation_rors:

  Aggregates distinct affiliation ROR identifiers for DataCite works found in
  DataCite and OpenAlex, grouped by DOI. The DataCite affiliation_identifier
  field contains a variety of different identifiers, which are converted into
  ROR identifiers.
*/

MODEL (
  name datacite_index.affiliation_rors,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  doi,
  @array_agg_distinct(ror) AS affiliation_rors,
FROM (
  -- Only use ROR IDs for affiliation IDs

  -- DataCite
  -- Convert various identifiers (ROR, GRID, ISNI) to ROR
  SELECT work_doi AS doi, ror.index.ror_id AS ror
  FROM datacite.works_affiliations AS dwa
  INNER JOIN ror.index ON dwa.affiliation_identifier = ror.index.identifier

  UNION ALL

  -- OpenAlex
  SELECT doi, ror
  FROM datacite.works dw
  INNER JOIN openalex.works_affiliations owa ON dw.doi = owa.work_doi
  WHERE ror IS NOT NULL
)
GROUP BY doi;