MODEL (
  name openalex_index.openalex_index,
  dialect duckdb,
  kind FULL
);

CACHE TABLE ids_temp AS (
  SELECT DISTINCT doi
  FROM openalex_index.works_metadata
);

SELECT
  idx.doi,
  openalex_index.titles.title,
  openalex_index.abstracts.abstract,
  openalex_index.types.type,
  openalex_index.publication_dates.publication_date,
  openalex_index.updated_dates.updated_date,
  COALESCE(openalex_index.affiliation_rors.affiliation_rors, []) AS affiliation_rors,
  COALESCE(openalex_index.affiliation_names.affiliation_names, []) AS affiliation_names,
  COALESCE(openalex_index.author_names.author_names, []) AS author_names,
  COALESCE(openalex_index.author_orcids.author_orcids, []) AS author_orcids,
  COALESCE(openalex_index.award_ids.award_ids, []) AS award_ids,
  COALESCE(openalex_index.funder_ids.funder_ids, []) AS funder_ids,
  COALESCE(openalex_index.funder_names.funder_names, []) AS funder_names
FROM ids_temp AS idx
LEFT JOIN openalex_index.titles ON idx.doi = openalex_index.titles.doi
LEFT JOIN openalex_index.abstracts ON idx.doi = openalex_index.abstracts.doi
LEFT JOIN openalex_index.types ON idx.doi = openalex_index.types.doi
LEFT JOIN openalex_index.publication_dates ON idx.doi = openalex_index.publication_dates.doi
LEFT JOIN openalex_index.updated_dates ON idx.doi = openalex_index.updated_dates.doi
LEFT JOIN openalex_index.affiliation_names ON idx.doi = openalex_index.affiliation_names.doi
LEFT JOIN openalex_index.affiliation_rors ON idx.doi = openalex_index.affiliation_rors.doi
LEFT JOIN openalex_index.author_names ON idx.doi = openalex_index.author_names.doi
LEFT JOIN openalex_index.author_orcids ON idx.doi = openalex_index.author_orcids.doi
LEFT JOIN openalex_index.award_ids ON idx.doi = openalex_index.award_ids.doi
LEFT JOIN openalex_index.funder_ids ON idx.doi = openalex_index.funder_ids.doi
LEFT JOIN openalex_index.funder_names ON idx.doi = openalex_index.funder_names.doi;

UNCACHE TABLE ids_temp;
DROP TABLE openalex_index.titles;

--@IF(
--  @runtime_stage = 'creating',
--  DROP TABLE openalex_index.titles;
--  DROP TABLE openalex_index.abstracts;
--  DROP TABLE openalex_index.types;
--  DROP TABLE openalex_index.publication_dates;
--  DROP TABLE openalex_index.updated_dates;
--  DROP TABLE openalex_index.affiliation_names;
--  DROP TABLE openalex_index.affiliation_rors;
--  DROP TABLE openalex_index.author_names;
--  DROP TABLE openalex_index.author_orcids;
--  DROP TABLE openalex_index.award_ids;
--  DROP TABLE openalex_index.funder_ids;
--  DROP TABLE openalex_index.funder_names;
--);
