CREATE OR REPLACE TABLE openalex_author_names AS
SELECT
  idx.doi,
  ARRAY_AGG_DISTINCT(display_name) AS author_names,
FROM openalex_meta AS idx
INNER JOIN openalex_works_authors ON idx.id = work_id
WHERE display_name IS NOT NULL
GROUP BY idx.doi