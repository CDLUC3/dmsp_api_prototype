CREATE TEMP TABLE openalex_ids_temp AS (
  -- Choose the id with the longest title for each duplicate DOI
  SELECT
    COALESCE(ARG_MAX(oam.id, oam.title_length), MIN(id)) AS id,
    oam.doi,
    MAX(oam.title_length) AS title_length
  FROM openalex_meta oam
  LEFT JOIN crossref_meta cfm ON oam.doi = cfm.doi
  WHERE oam.is_duplicate = TRUE AND (oam.title_length > 0 OR cfm.title_length > 0)
  GROUP BY oam.doi

  UNION ALL

  -- Select all remaining id, doi pairs
  SELECT
    oam.id,
    oam.doi,
    oam.title_length
  FROM openalex_meta oam
  LEFT JOIN crossref_meta cfm ON oam.doi = cfm.doi
  WHERE oam.is_duplicate = FALSE AND (oam.title_length > 0 OR cfm.title_length > 0)
);

CREATE OR REPLACE TABLE openalex_titles AS
SELECT
  idx.doi,
  CASE
    WHEN cfm.title_length > idx.title_length THEN cfw.title
    ELSE oaw.title
  END AS title
FROM openalex_ids_temp AS idx
LEFT JOIN openalex_works oaw ON idx.id = oaw.id
LEFT JOIN crossref_meta cfm ON idx.doi = cfm.doi
LEFT JOIN crossref_works cfw ON idx.doi = cfw.doi