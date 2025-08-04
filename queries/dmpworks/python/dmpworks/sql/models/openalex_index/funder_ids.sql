/*
  openalex_index.funder_ids:

  Aggregates distinct funder identifiers from OpenAlex and Crossref Metadata,
  grouped by DOI. OpenAlex funder IDs are converted into both Crossref Funder IDs
  and ROR IDs. Crossref Funder IDs from Crossref Metadata are kept as they are
  and also converted into ROR IDs. Grouping by DOI also handles cases where
  multiple OpenAlex records share the same DOI. DataCite works are excluded via
  openalex_index.works_metadata.
*/

MODEL (
  name openalex_index.funder_ids,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  doi,
  @array_agg_distinct(funder_id) AS funder_ids
FROM (
  -- OpenAlex
  -- Get OpenAlex funder Crossref Funder IDs
  SELECT owm.id, owm.doi, ids.doi AS funder_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN openalex.works_funders owf ON owm.id = owf.work_id
  LEFT JOIN openalex.funders oaf ON owf.funder_id = oaf.id
  WHERE ids.doi IS NOT NULL

  UNION ALL

  -- Get ROR IDs
  -- Get OpenAlex funder ROR IDs
  SELECT owm.id, owm.doi, ids.ror AS funder_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN openalex.works_funders owf ON owm.id = owf.work_id
  LEFT JOIN openalex.funders oaf ON owf.funder_id = oaf.id
  WHERE ids.ror IS NOT NULL

  UNION ALL

  -- Crossref Metadata: Crossref Funder IDs
  SELECT owm.id, owm.doi, funder_doi AS funder_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN crossref_metadata.works_funders cwf ON owm.doi = cwf.work_doi
  WHERE funder_doi IS NOT NULL

  UNION ALL

  -- Crossref Metadata: convert Crossref Funder IDs to RORs
  SELECT owm.id, owm.doi, ror.index.ror_id AS funder_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN crossref_metadata.works_funders cwf ON owm.doi = cwf.work_doi
  INNER JOIN ror.index ON cwf.funder_doi = ror.index.identifier
  WHERE funder_doi IS NOT NULL
)
GROUP BY doi;