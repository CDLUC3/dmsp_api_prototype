MODEL (
  name datacite_index.award_ids,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  @array_agg_distinct(award_id) AS award_ids,
FROM (
  -- DataCite
  SELECT work_doi AS doi, award_number AS award_id
  FROM datacite.works_funders
  WHERE award_number IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, award_id
  FROM datacite.works dw
  INNER JOIN openalex.works_funders owf ON dw.doi = owf.work_doi
  WHERE award_id IS NOT NULL
)
GROUP BY doi;