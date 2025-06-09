MODEL (
  name openalex_index.publication_dates,
  dialect duckdb,
  kind FULL
);

SELECT
  owm.doi,
  MAX(publication_date) AS publication_date
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works ow ON owm.id = ow.id
WHERE publication_date IS NOT NULL
GROUP BY owm.doi;