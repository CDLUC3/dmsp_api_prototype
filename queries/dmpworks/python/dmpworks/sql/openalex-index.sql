CREATE TEMP TABLE openalex_ids_temp AS (
  SELECT DISTINCT doi
  FROM openalex_meta
);

CREATE OR REPLACE TABLE openalex_index AS
SELECT
  idx.doi,
  openalex_titles.title,
  openalex_abstracts.abstract,
  openalex_types.type,
  openalex_publication_dates.publication_date,
  openalex_updated_dates.updated_date,
  COALESCE(openalex_affiliation_rors.affiliation_rors, []) AS affiliation_rors,
  COALESCE(openalex_affiliation_names.affiliation_names, []) AS affiliation_names,
  COALESCE(openalex_author_names.author_names, []) AS author_names,
  COALESCE(openalex_author_orcids.author_orcids, []) AS author_orcids,
  COALESCE(openalex_award_ids.award_ids, []) AS award_ids,
  COALESCE(openalex_funder_ids.funder_ids, []) AS funder_ids,
  COALESCE(openalex_funder_names.funder_names, []) AS funder_names
FROM openalex_ids_temp AS idx
LEFT JOIN openalex_titles ON idx.doi = openalex_titles.doi
LEFT JOIN openalex_abstracts ON idx.doi = openalex_abstracts.doi
LEFT JOIN openalex_types ON idx.doi = openalex_types.doi
LEFT JOIN openalex_publication_dates ON idx.doi = openalex_publication_dates.doi
LEFT JOIN openalex_updated_dates ON idx.doi = openalex_updated_dates.doi
LEFT JOIN openalex_affiliation_names ON idx.doi = openalex_affiliation_names.doi
LEFT JOIN openalex_affiliation_rors ON idx.doi = openalex_affiliation_rors.doi
LEFT JOIN openalex_author_names ON idx.doi = openalex_author_names.doi
LEFT JOIN openalex_author_orcids ON idx.doi = openalex_author_orcids.doi
LEFT JOIN openalex_award_ids ON idx.doi = openalex_award_ids.doi
LEFT JOIN openalex_funder_ids ON idx.doi = openalex_funder_ids.doi
LEFT JOIN openalex_funder_names ON idx.doi = openalex_funder_names.doi



