/*
  datacite_index.funder_ids:

  Aggregates distinct funder identifiers for DataCite works, from DataCite and
  OpenAlex, grouped by DOI. DateCite contains a variety of funder identifiers,
  the Crossref funder identifiers are kept, then all funder identifiers are also
  converted into ROR identifiers. OpenAlex funder IDs are converted into both
  Crossref Funder IDs and ROR IDs. Grouping by DOI also handles cases where
  multiple OpenAlex records share the same DOI. DataCite works are excluded via
  openalex_index.works_metadata.
*/

MODEL (
  name datacite_index.funder_ids,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  @array_agg_distinct(id) AS funder_ids,
FROM (
  -- Include ROR and Crossref Funder IDs for funder identifiers

  -- DataCite
  -- Include DataCite funder Crossref Funder IDs
  SELECT work_doi AS doi, funder_identifier AS id
  FROM datacite.works_funders
  WHERE funder_identifier_type = 'Crossref Funder ID' AND funder_identifier IS NOT NULL

  UNION ALL

  -- Convert DataCite funder IDs to ROR IDs
  SELECT work_doi AS doi, ror.index.ror_id AS id
  FROM datacite.works_funders
  INNER JOIN ror.index ON funder_identifier = ror.index.identifier
  WHERE ror.index.ror_id IS NOT NULL

  UNION ALL

  -- OpenAlex
  -- Get OpenAlex funder Crossref Funder IDs
  SELECT doi, ids.doi AS id
  FROM datacite.works dw
  INNER JOIN openalex.works_funders owf ON dw.doi = owf.work_doi
  INNER JOIN openalex.funders oaf ON owf.funder_id = oaf.id
  WHERE ids.doi IS NOT NULL

  UNION ALL

  -- Get ROR IDs
  -- Get OpenAlex funder ROR IDs
  SELECT doi, ids.ror AS id
  FROM datacite.works dw
  INNER JOIN openalex.works_funders owf ON dw.doi = owf.work_doi
  INNER JOIN openalex.funders oaf ON owf.funder_id = oaf.id
  WHERE ids.ror IS NOT NULL
)
GROUP BY doi;