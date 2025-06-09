MODEL (
  name openalex_index.titles,
  dialect duckdb,
  kind FULL
);

CACHE TABLE ids_temp AS (
  -- Choose the id with the longest title for each duplicate DOI
  SELECT
    COALESCE(ARG_MAX(owm.id, owm.title_length), MIN(id)) AS id,
    owm.doi,
    MAX(owm.title_length) AS title_length
  FROM openalex_index.works_metadata owm
  LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
  WHERE owm.is_duplicate = TRUE AND (owm.title_length > 0 OR cwm.title_length > 0)
  GROUP BY owm.doi

  UNION ALL

  -- Select all remaining id, doi pairs
  SELECT
    owm.id,
    owm.doi,
    owm.title_length
  FROM openalex_index.works_metadata owm
  LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
  WHERE owm.is_duplicate = FALSE AND (owm.title_length > 0 OR cwm.title_length > 0)
);

SELECT
  owm.doi,
  CASE
    WHEN cwm.title_length > owm.title_length THEN cfw.title
    ELSE oaw.title
  END AS title
FROM ids_temp AS owm
LEFT JOIN openalex.works oaw ON owm.id = oaw.id
LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
LEFT JOIN crossref.works cfw ON owm.doi = cfw.doi;

UNCACHE TABLE ids_temp;