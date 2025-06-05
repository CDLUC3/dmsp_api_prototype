-- Normalise OpenAlex

CREATE OR REPLACE TABLE openalex_index AS
WITH
idx AS (
  -- Remove DataCite works from OpenAlex
  SELECT
    id,
    doi
  FROM openalex_works
  WHERE doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM datacite_works WHERE openalex_works.doi = datacite_works.doi)
),
funder_ids AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(funder_id) AS funder_ids
  FROM (
    -- OpenAlex
    -- Get OpenAlex funder Crossref Funder IDs
    SELECT idx.id, idx.doi, oa_fndrs.ids.doi AS funder_id
    FROM idx
    LEFT JOIN openalex_works_funders AS oaw_fndrs ON idx.id = oaw_fndrs.work_id
    LEFT JOIN openalex_funders AS oa_fndrs ON oaw_fndrs.funder_id = oa_fndrs.id
    WHERE oa_fndrs.ids.doi IS NOT NULL

    UNION

    -- Get ROR IDs
    -- Get OpenAlex funder ROR IDs
    SELECT idx.id, idx.doi, oa_fndrs.ids.ror AS funder_id
    FROM idx
    LEFT JOIN openalex_works_funders AS oaw_fndrs ON idx.id = oaw_fndrs.work_id
    LEFT JOIN openalex_funders AS oa_fndrs ON oaw_fndrs.funder_id = oa_fndrs.id
    WHERE oa_fndrs.ids.ror IS NOT NULL

    UNION

    -- Crossref Metadata
    SELECT idx.id, idx.doi, cfw_fndrs.funder_doi AS funder_id
    FROM idx
    LEFT JOIN crossref_works_funders AS cfw_fndrs ON idx.doi = cfw_fndrs.work_doi
  )
  GROUP BY doi
),
funder_names AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(funder_name) AS funder_names
  FROM (
    -- OpenAlex
    SELECT idx.id, idx.doi, oaw_fndrs.funder_display_name AS funder_name
    FROM idx
    LEFT JOIN openalex_works_funders AS oaw_fndrs ON idx.id = oaw_fndrs.work_id

    UNION

    -- Crossref Metadata
    SELECT idx.id, idx.doi, cfw_fndrs.name AS funder_name
    FROM idx
    LEFT JOIN crossref_works_funders AS cfw_fndrs ON idx.doi = cfw_fndrs.work_doi
  )
  GROUP BY doi
),
award_ids AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(award_id) AS award_ids
  FROM (
    -- OpenAlex
    SELECT idx.id, idx.doi, oaw_fndrs.award_id
    FROM idx
    LEFT JOIN openalex_works_funders AS oaw_fndrs ON idx.id = oaw_fndrs.work_id

    UNION

    -- Crossref Metadata
    SELECT idx.id, idx.doi, cfw_fndrs.award AS award_id
    FROM idx
    LEFT JOIN crossref_works_funders AS cfw_fndrs ON idx.doi = cfw_fndrs.work_doi
  )
  GROUP BY doi
),
updated_dates AS (
  -- Choose most recent update_date from OpenAlex and Crossref Metadata
  SELECT
    doi,
    MAX(updated_date) AS updated_date
  FROM (
    SELECT idx.doi, oaw.updated_date
    FROM idx
    LEFT JOIN openalex_works AS oaw ON idx.id = oaw.id

    UNION

    SELECT doi, updated_date
    FROM crossref_works
    WHERE doi IS NOT NULL
  )
  GROUP BY doi
),
works_merged AS (
  -- OpenAlex contains duplicate works with the same DOI. Sometimes one work has more metadata than others, so group and merge all data together.
  SELECT
    doi,
    MIN(title) AS title, -- Could change to longest
    MIN(abstract) AS abstract, -- Could change to longest
    MIN(type) AS type, -- Could change to most common
    MAX(publication_date) AS publication_date,
    ARRAY_AGG_DISTINCT(affiliation_ror) AS affiliation_rors,
    ARRAY_AGG_DISTINCT(affiliation_name) AS affiliation_names,
    ARRAY_AGG_DISTINCT(author_name) AS author_names,
    ARRAY_AGG_DISTINCT(author_orcid) AS author_orcids,
  FROM (
    SELECT
      idx.doi,
      oaw.title,
      oaw.abstract,
      oaw.type,
      oaw.publication_date,
      oaw.updated_date,
      oaw_affl.ror AS affiliation_ror,
      oaw_affl.display_name AS affiliation_name,
      oaw_auth.display_name AS author_name,
      oaw_auth.orcid AS author_orcid,
    FROM idx
    LEFT JOIN openalex_works AS oaw ON idx.id = oaw.id
    LEFT JOIN openalex_works_affiliations AS oaw_affl ON idx.id = oaw_affl.work_id
    LEFT JOIN openalex_works_authors AS oaw_auth ON idx.id = oaw_auth.work_id
  ) AS subquery
  GROUP BY doi
)
SELECT
  works.doi,
  COALESCE(crossref_works.title, works.title) AS title,
  COALESCE(crossref_works.abstract, works.abstract) AS abstract,
  works.type,
  works.publication_date,
  updated_dates.updated_date,
  works.affiliation_rors,
  works.affiliation_names,
  works.author_names,
  works.author_orcids,
  funder_ids.funder_ids,
  funder_names.funder_names,
  award_ids.award_ids
FROM works_merged AS works
LEFT JOIN funder_ids ON works.doi = funder_ids.doi
LEFT JOIN funder_names ON works.doi = funder_names.doi
LEFT JOIN award_ids ON works.doi = award_ids.doi
LEFT JOIN updated_dates ON works.doi = updated_dates.doi
LEFT JOIN crossref_works ON works.doi = crossref_works.doi