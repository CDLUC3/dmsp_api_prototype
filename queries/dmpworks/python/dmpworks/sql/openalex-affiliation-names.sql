CREATE OR REPLACE TABLE openalex_affiliation_names AS
SELECT
  idx.doi,
  ARRAY_AGG_DISTINCT(display_name) AS affiliation_names,
FROM openalex_meta AS idx
INNER JOIN openalex_works_affiliations ON idx.id = work_id
WHERE display_name IS NOT NULL
GROUP BY idx.doi