/*
  openalex_index.openalex_index:

  Creates the OpenAlex index table.
*/

MODEL (
  name openalex_index.openalex_index,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('openalex_index_openalex_index_threads') AS INT64);

JINJA_QUERY_BEGIN;

WITH dois AS (
  SELECT DISTINCT doi
  FROM openalex_index.works_metadata
)

SELECT
  dois.doi,
  openalex_index.titles.title,
  {% if var('include_abstracts') %}
  openalex_index.abstracts.abstract,
  {% endif %}
  COALESCE(openalex_index.types.type, 'other') AS type,
  openalex_index.publication_dates.publication_date,
  openalex_index.updated_dates.updated_date,
  openalex.works.institutions,
  openalex.works.authors,
  COALESCE(openalex_index.funders.funders, []) AS funders,
  COALESCE(openalex_index.award_ids.award_ids, []) AS award_ids,
FROM dois
LEFT JOIN openalex_index.titles ON dois.doi = openalex_index.titles.doi
{% if var('include_abstracts') %}
LEFT JOIN openalex_index.abstracts ON dois.doi = openalex_index.abstracts.doi
{% endif %}
LEFT JOIN openalex_index.types ON dois.doi = openalex_index.types.doi
LEFT JOIN openalex_index.publication_dates ON dois.doi = openalex_index.publication_dates.doi
LEFT JOIN openalex_index.updated_dates ON dois.doi = openalex_index.updated_dates.doi
LEFT JOIN openalex_index.award_ids ON dois.doi = openalex_index.award_ids.doi
LEFT JOIN openalex_index.funders ON dois.doi = openalex_index.funders.doi
-- TODO: join with openalex works

JINJA_END;
