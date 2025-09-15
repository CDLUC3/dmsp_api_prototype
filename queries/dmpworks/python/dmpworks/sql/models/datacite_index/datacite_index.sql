/*
  datacite_index.datacite_index:

  Creates the DataCite index table.
*/

MODEL (
  name datacite_index.datacite_index,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  dw.doi,
  dw.title,
  dw.abstract,
  COALESCE(datacite_index.types.type, 'other') AS type,
  dw.publication_date,
  datacite_index.updated_dates.updated_date,
  dw.publication_venue,
  COALESCE(datacite_index.institutions.institutions, []) AS institutions,
  dw.authors,
  COALESCE(datacite_index.funders.funders, []) AS funders,
  COALESCE(datacite_index.award_ids.award_ids, []) AS award_ids,
  {name := 'DataCite', url := 'https://commons.datacite.org/doi.org/' || dw.doi} AS source
FROM datacite_index.works dw
LEFT JOIN datacite_index.types ON dw.doi = datacite_index.types.doi
LEFT JOIN datacite_index.updated_dates ON dw.doi = datacite_index.updated_dates.doi
LEFT JOIN datacite_index.institutions ON dw.doi = datacite_index.institutions.doi
LEFT JOIN datacite_index.funders ON dw.doi = datacite_index.funders.doi
LEFT JOIN datacite_index.award_ids ON dw.doi = datacite_index.award_ids.doi
