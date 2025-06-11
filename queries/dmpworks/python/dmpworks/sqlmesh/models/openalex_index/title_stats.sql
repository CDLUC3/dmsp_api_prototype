MODEL (
  name openalex_index.title_stats,
  dialect duckdb,
  kind FULL
);

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