/*
  openalex_index.works_metadata:

  Filters out OpenAlex works that are also present in DataCite, and then
  collates metadata for the remaining works — including OpenAlex ID, DOI, title
  length, abstract length and a duplicate flag (whether another OpenAlex work
  shares the same DOI).

  This table is used by downstream queries as the leftmost table in joins, so
  that non-DataCite OpenAlex works are used in further processing.
*/

MODEL (
  name openalex_index.works_metadata,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Remove works that can be found in DataCite
WITH base AS (
  SELECT
    id,
    doi
  FROM openalex.works oaw
  WHERE doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM datacite.works WHERE oaw.doi = datacite.works.doi)
),
-- Count how many instances of each DOI
counts AS (
  SELECT
    doi,
    COUNT(*) AS doi_count
  FROM base
  GROUP BY doi
)
-- Collate information
SELECT
  base.id,
  base.doi,
  LENGTH(oaw.title) AS title_length,
  LENGTH(oaw.abstract) AS abstract_length,
  counts.doi_count > 1 AS is_duplicate
FROM base
LEFT JOIN counts ON base.doi = counts.doi
LEFT JOIN openalex.works oaw ON base.id = oaw.id;