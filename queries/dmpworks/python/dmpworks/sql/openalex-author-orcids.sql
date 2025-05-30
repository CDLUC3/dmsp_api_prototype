CREATE OR REPLACE TABLE openalex_author_orcids AS
SELECT
  idx.doi,
  ARRAY_AGG_DISTINCT(orcid) AS author_orcids,
FROM openalex_meta AS idx
INNER JOIN openalex_works_authors ON idx.id = work_id
WHERE orcid IS NOT NULL
GROUP BY idx.doi