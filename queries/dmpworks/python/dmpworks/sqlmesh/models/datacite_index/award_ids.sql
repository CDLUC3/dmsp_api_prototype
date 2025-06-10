MODEL (
  name datacite_index.award_ids,
  dialect duckdb,
  kind FULL
);

JINJA_QUERY_BEGIN;
SELECT
  doi,
  {{ array_agg_distinct('award_id') }} AS award_ids,
FROM (
  -- DataCite
  SELECT doi, award_number AS award_id
  FROM datacite.works dw
  INNER JOIN datacite.works_funders dwf ON dw.doi = dwf.work_doi
  WHERE award_number IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, award_id
  FROM datacite.works dw
  INNER JOIN openalex.works_funders owf ON dw.doi = owf.work_doi
  WHERE award_id IS NOT NULL
)
GROUP BY doi;
JINJA_END;