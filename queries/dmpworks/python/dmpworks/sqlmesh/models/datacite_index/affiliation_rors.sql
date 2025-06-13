MODEL (
  name datacite_index.affiliation_rors,
  dialect duckdb,
  kind FULL
);

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