MODEL (
  name openalex_index.funder_ids,
  dialect duckdb,
  kind FULL
);

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

  -- Crossref Metadata
  SELECT owm.id, owm.doi, funder_doi AS funder_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN crossref.works_funders cwf ON owm.doi = cwf.work_doi
  WHERE funder_doi IS NOT NULL
)
GROUP BY doi;