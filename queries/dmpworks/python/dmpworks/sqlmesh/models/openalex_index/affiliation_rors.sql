/*
  openalex_index.affiliation_rors:

  Aggregates distinct affiliation ROR identifiers for each OpenAlex work, grouped by DOI.
  Grouping by DOI also handles cases where multiple OpenAlex records share the same DOI.
  DataCite works are excluded via openalex_index.works_metadata.
*/

MODEL (
  name openalex_index.affiliation_rors,
  dialect duckdb,
  kind FULL
);

SELECT
  owm.doi,
  @array_agg_distinct(ror) AS affiliation_rors,
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works_affiliations ON owm.id = work_id
WHERE ror IS NOT NULL
GROUP BY owm.doi;
