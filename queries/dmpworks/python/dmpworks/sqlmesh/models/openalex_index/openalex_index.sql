/*
  openalex_index.openalex_index:

  Creates the OpenAlex index table.
*/

MODEL (
  name openalex_index.openalex_index,
  dialect duckdb,
  kind FULL
);

WITH dois AS (
  SELECT DISTINCT doi
  FROM openalex_index.works_metadata
)

SELECT
  dois.doi,
  openalex_index.titles.title,
  openalex_index.abstracts.abstract,
  COALESCE(openalex_index.types.type, 'other') AS type,
  openalex_index.publication_dates.publication_date,
  openalex_index.updated_dates.updated_date,
  COALESCE(openalex_index.affiliation_rors.affiliation_rors, []) AS affiliation_rors,
  COALESCE(openalex_index.affiliation_names.affiliation_names, []) AS affiliation_names,
  COALESCE(openalex_index.author_names.author_names, []) AS author_names,
  COALESCE(openalex_index.author_orcids.author_orcids, []) AS author_orcids,
  COALESCE(openalex_index.award_ids.award_ids, []) AS award_ids,
  COALESCE(openalex_index.funder_ids.funder_ids, []) AS funder_ids,
  COALESCE(openalex_index.funder_names.funder_names, []) AS funder_names
FROM dois
LEFT JOIN openalex_index.titles ON dois.doi = openalex_index.titles.doi
LEFT JOIN openalex_index.abstracts ON dois.doi = openalex_index.abstracts.doi
LEFT JOIN openalex_index.types ON dois.doi = openalex_index.types.doi
LEFT JOIN openalex_index.publication_dates ON dois.doi = openalex_index.publication_dates.doi
LEFT JOIN openalex_index.updated_dates ON dois.doi = openalex_index.updated_dates.doi
LEFT JOIN openalex_index.affiliation_names ON dois.doi = openalex_index.affiliation_names.doi
LEFT JOIN openalex_index.affiliation_rors ON dois.doi = openalex_index.affiliation_rors.doi
LEFT JOIN openalex_index.author_names ON dois.doi = openalex_index.author_names.doi
LEFT JOIN openalex_index.author_orcids ON dois.doi = openalex_index.author_orcids.doi
LEFT JOIN openalex_index.award_ids ON dois.doi = openalex_index.award_ids.doi
LEFT JOIN openalex_index.funder_ids ON dois.doi = openalex_index.funder_ids.doi
LEFT JOIN openalex_index.funder_names ON dois.doi = openalex_index.funder_names.doi;

