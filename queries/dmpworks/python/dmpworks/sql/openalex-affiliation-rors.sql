CREATE OR REPLACE TABLE openalex_affiliation_rors AS
SELECT
  idx.doi,
  ARRAY_AGG_DISTINCT(ror) AS affiliation_rors,
FROM openalex_meta AS idx
INNER JOIN openalex_works_affiliations ON idx.id = work_id
WHERE ror IS NOT NULL
GROUP BY idx.doi