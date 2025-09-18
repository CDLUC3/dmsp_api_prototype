/*
  datacite_index.datacite_index:

  Creates the DataCite index table.
*/


MODEL (
  name datacite_index.datacite_index,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

JINJA_QUERY_BEGIN;

SELECT
  datacite.works.doi,
  datacite.works.title,
  {% if var('include_abstracts') %}
  datacite.works.abstract,
  {% endif %}
  COALESCE(datacite_index.types.type, 'other') AS type,
  datacite.works.publication_date,
  datacite_index.updated_dates.updated_date,
  COALESCE(datacite_index.institutions.institutions, []) AS institutions,
  datacite.works.authors, -- TODO: could try to incorporate OpenAlex info here
  COALESCE(datacite_index.funders.funders, []) AS funders,
  COALESCE(datacite_index.award_ids.award_ids, []) AS award_ids,
FROM datacite.works
LEFT JOIN datacite_index.types ON datacite.works.doi = datacite_index.types.doi
LEFT JOIN datacite_index.updated_dates ON datacite.works.doi = datacite_index.updated_dates.doi
LEFT JOIN datacite_index.institutions ON datacite.works.doi = datacite_index.institutions.doi
LEFT JOIN datacite_index.authors ON datacite.works.doi = datacite_index.authors.doi
LEFT JOIN datacite_index.funders ON datacite.works.doi = datacite_index.funders.doi
LEFT JOIN datacite_index.award_ids ON datacite.works.doi = datacite_index.award_ids.doi

JINJA_END;
