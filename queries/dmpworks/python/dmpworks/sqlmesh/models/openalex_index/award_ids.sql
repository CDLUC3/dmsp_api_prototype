MODEL (
  name openalex_index.award_ids,
  dialect duckdb,
  kind FULL
);

JINJA_QUERY_BEGIN;
SELECT
  doi,
  {{ array_agg_distinct('award_id') }} AS award_ids
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
  INNER JOIN crossref.works_funders ON owm.doi = work_doi
  WHERE award IS NOT NULL
)
GROUP BY doi;
JINJA_END;