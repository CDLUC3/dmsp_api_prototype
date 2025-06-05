CREATE OR REPLACE TABLE openalex_types AS
SELECT
  idx.doi,
  MIN(type) AS type, -- Could change to most common
FROM openalex_meta AS idx
INNER JOIN openalex_works ow ON idx.id = ow.id
WHERE type IS NOT NULL
GROUP BY idx.doi