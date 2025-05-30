CREATE OR REPLACE TABLE openalex_publication_dates AS
SELECT
  idx.doi,
  MAX(publication_date) AS publication_date
FROM openalex_meta AS idx
INNER JOIN openalex_works ow ON idx.id = ow.id
WHERE publication_date IS NOT NULL
GROUP BY idx.doi