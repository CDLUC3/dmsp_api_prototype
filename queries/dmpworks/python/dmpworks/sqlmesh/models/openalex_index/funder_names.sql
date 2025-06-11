MODEL (
  name openalex_index.funder_names,
  dialect duckdb,
  kind FULL
);

JINJA_QUERY_BEGIN;
SELECT
  doi,
  {{ array_agg_distinct('funder_name') }} AS funder_names
FROM (
  -- OpenAlex
  SELECT owm.id, owm.doi, funder_display_name AS funder_name
  FROM openalex_index.works_metadata AS owm
  INNER JOIN openalex.works_funders owf ON owm.id = owf.work_id
  WHERE funder_display_name IS NOT NULL

  UNION ALL

  -- Crossref Metadata
  SELECT owm.id, owm.doi, name AS funder_name
  FROM openalex_index.works_metadata AS owm
  INNER JOIN crossref.works_funders cwf ON owm.doi = cwf.work_doi
  WHERE name IS NOT NULL
)
GROUP BY doi;
JINJA_END;