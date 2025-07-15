/*
  datacite_index.datacite_index:

  Creates the DataCite index table.
*/


MODEL (
  name datacite_index.datacite_index,
  dialect duckdb,
  kind FULL
);

SELECT
  datacite.works.doi,
  datacite.works.title,
  datacite.works.abstract,
  COALESCE(datacite_index.types.type, 'other') AS type,
  datacite.works.publication_date,
  datacite_index.updated_dates.updated_date,
  COALESCE(datacite_index.affiliation_rors.affiliation_rors, []) AS affiliation_rors,
  COALESCE(datacite_index.affiliation_names.affiliation_names, []) AS affiliation_names,
  COALESCE(datacite_index.author_names.author_names, []) AS author_names,
  COALESCE(datacite_index.author_orcids.author_orcids, []) AS author_orcids,
  COALESCE(datacite_index.award_ids.award_ids, []) AS award_ids,
  COALESCE(datacite_index.funder_ids.funder_ids, []) AS funder_ids,
  COALESCE(datacite_index.funder_names.funder_names, []) AS funder_names
FROM datacite.works
LEFT JOIN datacite_index.types ON datacite.works.doi = datacite_index.types.doi
LEFT JOIN datacite_index.updated_dates ON datacite.works.doi = datacite_index.updated_dates.doi
LEFT JOIN datacite_index.affiliation_names ON datacite.works.doi = datacite_index.affiliation_names.doi
LEFT JOIN datacite_index.affiliation_rors ON datacite.works.doi = datacite_index.affiliation_rors.doi
LEFT JOIN datacite_index.author_names ON datacite.works.doi = datacite_index.author_names.doi
LEFT JOIN datacite_index.author_orcids ON datacite.works.doi = datacite_index.author_orcids.doi
LEFT JOIN datacite_index.award_ids ON datacite.works.doi = datacite_index.award_ids.doi
LEFT JOIN datacite_index.funder_ids ON datacite.works.doi = datacite_index.funder_ids.doi
LEFT JOIN datacite_index.funder_names ON datacite.works.doi = datacite_index.funder_names.doi;
