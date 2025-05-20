-- Normalise DataCite

CREATE OR REPLACE TABLE datacite_index AS
WITH affiliation_rors AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(ror) AS affiliation_rors,
  FROM (
    -- Only use ROR IDs for affiliation IDs

    -- DataCite
    -- Convert various identifiers (ROR, GRID, ISNI) to ROR
    SELECT doi, ror_index.identifier AS ror
    FROM datacite_works
    LEFT JOIN datacite_works_affiliations AS dwa ON doi = dwa.work_doi
    LEFT JOIN ror_index ON dwa.affiliation_identifier = ror_index.identifier

    UNION

    -- OpenAlex
    SELECT doi, ror
    FROM datacite_works
    LEFT JOIN openalex_works_affiliations ON doi = work_doi
  )
  GROUP BY doi
),
affiliation_names AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(name) AS affiliation_names,
  FROM (
    -- DataCite
    SELECT doi, name
    FROM datacite_works
    LEFT JOIN datacite_works_affiliations ON doi = work_doi

    UNION

    -- OpenAlex
    SELECT doi, display_name AS name
    FROM datacite_works
    LEFT JOIN openalex_works_affiliations ON doi = work_doi
  )
  GROUP BY doi
),
author_names AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(name) AS author_names,
  FROM (
    -- DataCite
    SELECT doi, name -- TODO: given_name, family_name, name
    FROM datacite_works
    LEFT JOIN datacite_works_authors ON doi = work_doi

    UNION

    -- OpenAlex
    SELECT doi, display_name AS name
    FROM datacite_works
    LEFT JOIN openalex_works_authors ON doi = work_doi
  )
  GROUP BY doi
),
author_orcids AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(orcid) AS author_orcids,
  FROM (
    -- Only use ORCID IDs for author identifiers

    -- DataCite
    SELECT doi, orcid
    FROM datacite_works
    LEFT JOIN datacite_works_authors ON doi = work_doi

    UNION

    -- OpenAlex
    SELECT doi, orcid
    FROM datacite_works
    LEFT JOIN openalex_works_authors ON doi = work_doi
  )
  GROUP BY doi
),
award_ids AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(award_id) AS award_ids,
  FROM (
    -- DataCite
    SELECT doi, award_number AS award_id
    FROM datacite_works
    LEFT JOIN datacite_works_funders ON doi = work_doi

    UNION

    -- OpenAlex
    SELECT doi, award_id
    FROM datacite_works
    LEFT JOIN openalex_works_funders ON doi = work_doi
  )
  GROUP BY doi
),
funder_ids AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(id) AS funder_ids,
  FROM (
    -- Include ROR and Crossref Funder IDs for funder identifiers

    -- DataCite
    -- Include DataCite funder Crossref Funder IDs
    SELECT doi, dwf.funder_identifier AS id
    FROM datacite_works
    LEFT JOIN datacite_works_funders AS dwf ON doi = dwf.work_doi
    WHERE dwf.funder_identifier_type = 'Crossref Funder ID'

    UNION

    -- Convert DataCite funder IDs to ROR IDs
    SELECT doi, ror_index.ror_id AS id
    FROM datacite_works
    LEFT JOIN datacite_works_funders AS dwf ON doi = dwf.work_doi
    LEFT JOIN ror_index ON dwf.funder_identifier = ror_index.identifier

    UNION

    -- OpenAlex
    -- Get OpenAlex funder Crossref Funder IDs
    SELECT doi, oa_fndrs.ids.doi AS id
    FROM datacite_works
    LEFT JOIN openalex_works_funders AS oaw_fndrs ON doi = work_doi
    LEFT JOIN openalex_funders AS oa_fndrs ON oaw_fndrs.funder_id = oa_fndrs.id
    WHERE oa_fndrs.ids.doi IS NOT NULL

    UNION

    -- Get ROR IDs
    -- Get OpenAlex funder ROR IDs
    SELECT doi, oa_fndrs.ids.ror AS id
    FROM datacite_works
    LEFT JOIN openalex_works_funders AS oaw_fndrs ON doi = work_doi
    LEFT JOIN openalex_funders AS oa_fndrs ON oaw_fndrs.funder_id = oa_fndrs.id
    WHERE oa_fndrs.ids.ror IS NOT NULL
  )
  GROUP BY doi
),
funder_names AS (
  SELECT
    doi,
    ARRAY_AGG_DISTINCT(name) AS funder_names,
  FROM (
    -- DataCite
    SELECT doi, funder_name AS name
    FROM datacite_works
    LEFT JOIN datacite_works_funders ON doi = work_doi

    UNION

    -- OpenAlex
    SELECT doi, funder_display_name AS name
    FROM datacite_works
    LEFT JOIN openalex_works_funders ON doi = work_doi
  )
  GROUP BY doi
),
-- Choose the most recent updated date from DataCite and OpenAlex
updated_dates AS (
  SELECT
    doi,
    MAX(updated_date) AS updated_date
  FROM (
    SELECT doi, updated_date
    FROM datacite_works

    UNION

    SELECT doi, updated_date
    FROM openalex_works
    WHERE doi IS NOT NULL
  )
  GROUP BY doi
),
type_map AS (
  -- Mapping table to normalise DataCite types
  SELECT * FROM (VALUES
    ('JournalArticle', 'article'),
    ('ConferencePaper', 'article'),
    ('Audiovisual', 'audio-visual'),
    ('Film', 'audio-visual'),
    ('Book', 'book'),
    ('BookChapter', 'book-chapter'),
    ('Collection', 'collection'),
    ('DataPaper', 'data-paper'),
    ('Dataset', 'dataset'),
    ('Dissertation', 'dissertation'),
    ('Event', 'event'),
    ('Image', 'image'),
    ('InteractiveResource', 'interactive-resource'),
    ('Model', 'model'),
    ('OutputManagementPlan', 'output-management-plan'),
    ('PeerReview', 'peer-review'),
    ('PhysicalObject', 'physical-object'),
    ('Instrument', 'physical-object'),
    ('Preprint', 'preprint'),
    ('Report', 'report'),
    ('Service', 'service'),
    ('Software', 'software'),
    ('ComputationalNotebook', 'software'),
    ('Sound', 'sound'),
    ('Standard', 'standard'),
    ('Text', 'text'),
    ('Workflow', 'workflow'),
    ('Other', 'other')
  ) AS t(original_type, normalized_type)
)
SELECT
  datacite_works.doi,
  datacite_works.title,
  datacite_works.abstract,
  COALESCE(type_map.normalized_type, 'other') AS type,
  datacite_works.publication_date,
  updated_dates.updated_date,
  affiliation_rors.affiliation_rors,
  affiliation_names.affiliation_names,
  author_names.author_names,
  author_orcids.author_orcids,
  award_ids.award_ids,
  funder_ids.funder_ids,
  funder_names.funder_names
FROM datacite_works
LEFT JOIN affiliation_rors ON datacite_works.doi = affiliation_rors.doi
LEFT JOIN affiliation_names ON datacite_works.doi = affiliation_names.doi
LEFT JOIN author_names ON datacite_works.doi = author_names.doi
LEFT JOIN author_orcids ON datacite_works.doi = author_orcids.doi
LEFT JOIN award_ids ON datacite_works.doi = award_ids.doi
LEFT JOIN funder_ids ON datacite_works.doi = funder_ids.doi
LEFT JOIN funder_names ON datacite_works.doi = funder_names.doi
LEFT JOIN updated_dates ON datacite_works.doi = updated_dates.doi
LEFT JOIN type_map ON datacite_works.type = type_map.original_type