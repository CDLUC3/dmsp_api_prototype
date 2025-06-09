MODEL (
  name openalex_index.updated_dates,
  dialect duckdb,
  kind FULL
);

-- Choose most recent update_date from OpenAlex and Crossref Metadata
SELECT
  doi,
  MAX(updated_date) AS updated_date
FROM (
  SELECT owm.doi, updated_date
  FROM openalex_index.works_metadata AS owm
  INNER JOIN openalex.works ow ON owm.id = ow.id
  WHERE updated_date IS NOT NULL

  UNION ALL

  SELECT doi, updated_date
  FROM crossref.works
  WHERE doi IS NOT NULL AND updated_date IS NOT NULL
)
GROUP BY doi;