MODEL (
  name openalex_index.author_orcids,
  dialect duckdb,
  kind FULL
);

SELECT
  owm.doi,
  @array_agg_distinct(orcid) AS author_orcids,
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works_authors ON owm.id = work_id
WHERE orcid IS NOT NULL
GROUP BY owm.doi;