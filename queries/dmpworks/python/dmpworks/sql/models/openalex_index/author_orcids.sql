/*
  openalex_index.author_orcids:

  Aggregates distinct affiliation author ORCID identifiers for each OpenAlex work,
  grouped by DOI. Grouping by DOI also handles cases where multiple OpenAlex
  records share the same DOI. DataCite works are excluded via
  openalex_index.works_metadata.
*/

MODEL (
  name openalex_index.author_orcids,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('openalex_index_author_orcids_threads') AS INT64);

SELECT
  owm.doi,
  @array_agg_distinct(orcid) AS author_orcids,
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works_authors ON owm.id = work_id
WHERE orcid IS NOT NULL
GROUP BY owm.doi;