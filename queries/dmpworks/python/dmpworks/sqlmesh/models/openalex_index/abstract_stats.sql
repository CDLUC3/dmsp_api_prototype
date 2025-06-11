MODEL (
  name openalex_index.abstract_stats,
  dialect duckdb,
  kind FULL
);

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