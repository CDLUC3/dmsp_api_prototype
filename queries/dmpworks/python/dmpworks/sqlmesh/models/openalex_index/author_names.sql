/*
  openalex_index.author_names:

  Aggregates distinct affiliation author names for each OpenAlex work, grouped
  by DOI. Grouping by DOI also handles cases where multiple OpenAlex records
  share the same DOI. DataCite works are excluded via openalex_index.works_metadata.
*/

MODEL (
  name openalex_index.author_names,
  dialect duckdb,
  kind FULL
);

SELECT
  owm.doi,
  @array_agg_distinct(display_name) AS author_names,
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works_authors ON owm.id = work_id
WHERE display_name IS NOT NULL
GROUP BY owm.doi;
