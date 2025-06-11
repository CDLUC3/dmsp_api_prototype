MODEL (
  name openalex_index.titles,
  dialect duckdb,
  kind FULL
);

SELECT
  stats.doi,
  CASE
    WHEN cwm.title_length > stats.title_length THEN cfw.title
    ELSE oaw.title
  END AS title
FROM openalex_index.title_stats AS stats
LEFT JOIN openalex.works oaw ON stats.id = oaw.id
LEFT JOIN crossref_index.works_metadata cwm ON stats.doi = cwm.doi
LEFT JOIN crossref.works cfw ON stats.doi = cfw.doi;
