/*
  datacite_index.award_ids:

  Aggregates distinct award identifiers for DataCite works found in DataCite and
  OpenAlex, grouped by DOI.
*/

MODEL (
  name datacite_index.award_ids,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

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