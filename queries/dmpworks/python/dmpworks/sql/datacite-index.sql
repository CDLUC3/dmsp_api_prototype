CREATE OR REPLACE TABLE datacite_index AS
SELECT
  datacite_works.doi,
  datacite_works.title,
  datacite_works.abstract,
  COALESCE(datacite_types.type, 'other') AS type,
  datacite_works.publication_date,
  datacite_updated_dates.updated_date,
  COALESCE(datacite_affiliation_rors.affiliation_rors, []) AS affiliation_rors,
  COALESCE(datacite_affiliation_names.affiliation_names, []) AS affiliation_names,
  COALESCE(datacite_author_names.author_names, []) AS author_names,
  COALESCE(datacite_author_orcids.author_orcids, []) AS author_orcids,
  COALESCE(datacite_award_ids.award_ids, []) AS award_ids,
  COALESCE(datacite_funder_ids.funder_ids, []) AS funder_ids,
  COALESCE(datacite_funder_names.funder_names, []) AS funder_names
FROM datacite_works
LEFT JOIN datacite_types ON datacite_works.doi = datacite_types.doi
LEFT JOIN datacite_updated_dates ON datacite_works.doi = datacite_updated_dates.doi
LEFT JOIN datacite_affiliation_names ON datacite_works.doi = datacite_affiliation_names.doi
LEFT JOIN datacite_affiliation_rors ON datacite_works.doi = datacite_affiliation_rors.doi
LEFT JOIN datacite_author_names ON datacite_works.doi = datacite_author_names.doi
LEFT JOIN datacite_author_orcids ON datacite_works.doi = datacite_author_orcids.doi
LEFT JOIN datacite_award_ids ON datacite_works.doi = datacite_award_ids.doi
LEFT JOIN datacite_funder_ids ON datacite_works.doi = datacite_funder_ids.doi
LEFT JOIN datacite_funder_names ON datacite_works.doi = datacite_funder_names.doi