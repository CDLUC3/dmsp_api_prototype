MODEL (
  name datacite_index.funder_names,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  @array_agg_distinct(name) AS funder_names,
FROM (
  -- DataCite
  SELECT work_doi AS doi, funder_name AS name
  FROM datacite.works_funders
  WHERE funder_name IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, funder_display_name AS name
  FROM datacite.works dw
  INNER JOIN openalex.works_funders owf ON dw.doi = owf.work_doi
  WHERE funder_display_name IS NOT NULL
)
GROUP BY doi;