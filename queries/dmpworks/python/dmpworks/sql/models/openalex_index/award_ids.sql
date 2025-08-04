/*
  openalex_index.award_ids:

  Aggregates distinct affiliation award identifiers for each OpenAlex and Crossref
  Metadata work, grouped by DOI. Grouping by DOI also handles cases where
  multiple OpenAlex records share the same DOI. DataCite works are excluded via
  openalex_index.works_metadata.
*/

MODEL (
  name openalex_index.award_ids,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  doi,
  @array_agg_distinct(award_id) AS award_ids
FROM (
  -- OpenAlex
  SELECT owm.id, owm.doi, award_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN openalex.works_funders ON owm.id = work_id
  WHERE award_id IS NOT NULL

  UNION ALL

  -- Crossref Metadata
  SELECT owm.id, owm.doi, award AS award_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN crossref_metadata.works_funders ON owm.doi = work_doi
  WHERE award IS NOT NULL
)
GROUP BY doi;