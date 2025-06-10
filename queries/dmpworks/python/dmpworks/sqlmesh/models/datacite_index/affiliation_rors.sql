MODEL (
  name datacite_index.affiliation_rors,
  dialect duckdb,
  kind FULL
);

JINJA_QUERY_BEGIN;
SELECT
  doi,
  {{ array_agg_distinct('ror') }} AS affiliation_rors,
FROM (
  -- Only use ROR IDs for affiliation IDs

  -- DataCite
  -- Convert various identifiers (ROR, GRID, ISNI) to ROR
  SELECT doi, ror.index.identifier AS ror
  FROM datacite.works dw
  INNER JOIN datacite.works_affiliations dwa ON dw.doi = dwa.work_doi
  INNER JOIN ror.index ON dwa.affiliation_identifier = ror.index.identifier

  UNION ALL

  -- OpenAlex
  SELECT doi, ror
  FROM datacite.works dw
  INNER JOIN openalex.works_affiliations owa ON dw.doi = owa.work_doi
  WHERE ror IS NOT NULL
)
GROUP BY doi;
JINJA_END;