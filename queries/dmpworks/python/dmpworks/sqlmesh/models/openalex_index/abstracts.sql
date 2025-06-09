MODEL (
  name openalex_index.abstracts,
  dialect duckdb,
  kind FULL
);

CACHE TABLE ids_temp AS (
  -- Choose the id with the longest abstract for each duplicate DOI
  SELECT
    COALESCE(ARG_MAX(owm.id, owm.abstract_length), MIN(id)) AS id,
    owm.doi,
    MAX(owm.abstract_length) AS abstract_length
  FROM openalex_index.works_metadata owm
  LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
  WHERE owm.is_duplicate = TRUE AND (owm.abstract_length > 0 OR cwm.abstract_length > 0)
  GROUP BY owm.doi

  UNION ALL

  -- Select all remaining id, doi pairs
  SELECT
    owm.id,
    owm.doi,
    owm.abstract_length
  FROM openalex_index.works_metadata owm
  LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
  WHERE owm.is_duplicate = FALSE AND (owm.abstract_length > 0 OR cwm.abstract_length > 0)
);

SELECT
  owm.doi,
  CASE
    WHEN cwm.abstract_length > owm.abstract_length THEN cfw.abstract
    ELSE oaw.abstract
  END AS abstract
FROM ids_temp AS owm
LEFT JOIN openalex.works oaw ON owm.id = oaw.id
LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
LEFT JOIN crossref.works cfw ON owm.doi = cfw.doi;

UNCACHE TABLE ids_temp;