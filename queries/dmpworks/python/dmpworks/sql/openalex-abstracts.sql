CREATE TEMP TABLE openalex_ids_temp AS (
  -- Choose the id with the longest abstract for each duplicate DOI
  SELECT
    COALESCE(ARG_MAX(oam.id, oam.abstract_length), MIN(id)) AS id,
    oam.doi,
    MAX(oam.abstract_length) AS abstract_length
  FROM openalex_meta oam
  LEFT JOIN crossref_meta cfm ON oam.doi = cfm.doi
  WHERE oam.is_duplicate = TRUE AND (oam.abstract_length > 0 OR cfm.abstract_length > 0)
  GROUP BY oam.doi

  UNION ALL

  -- Select all remaining id, doi pairs
  SELECT
    oam.id,
    oam.doi,
    oam.abstract_length
  FROM openalex_meta oam
  LEFT JOIN crossref_meta cfm ON oam.doi = cfm.doi
  WHERE oam.is_duplicate = FALSE AND (oam.abstract_length > 0 OR cfm.abstract_length > 0)
);

CREATE OR REPLACE TABLE openalex_abstracts AS
SELECT
  idx.doi,
  CASE
    WHEN cfm.abstract_length > idx.abstract_length THEN cfw.abstract
    ELSE oaw.abstract
  END AS abstract
FROM openalex_ids_temp AS idx
LEFT JOIN openalex_works oaw ON idx.id = oaw.id
LEFT JOIN crossref_meta cfm ON idx.doi = cfm.doi
LEFT JOIN crossref_works cfw ON idx.doi = cfw.doi