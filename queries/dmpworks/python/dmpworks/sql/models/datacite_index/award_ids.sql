/*
  datacite_index.award_ids:

  Aggregates distinct award identifiers for DataCite works found in DataCite and
  OpenAlex, grouped by DOI.
*/

MODEL (
  name datacite_index.award_ids,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  doi,
  @array_agg_distinct(award_id) AS award_ids,
FROM (
  -- DataCite
  SELECT doi, funder.award_number AS award_id
  FROM datacite_index.works, UNNEST(funders) AS item(funder)
  WHERE funder.award_number IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT dw.doi, fund.award_id
  FROM datacite_index.works dw
  INNER JOIN openalex.works ow ON dw.doi = ow.doi, UNNEST(ow.grants) AS item(fund)
  WHERE fund.award_id IS NOT NULL
)
GROUP BY doi;