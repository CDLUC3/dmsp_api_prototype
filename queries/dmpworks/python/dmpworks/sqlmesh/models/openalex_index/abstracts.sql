MODEL (
  name openalex_index.abstracts,
  dialect duckdb,
  kind FULL
);

SELECT
  stats.doi,
  CASE
    WHEN cwm.abstract_length > stats.abstract_length THEN cfw.abstract
    ELSE oaw.abstract
  END AS abstract
FROM openalex_index.abstract_stats AS stats
LEFT JOIN openalex.works oaw ON stats.id = oaw.id
LEFT JOIN crossref_index.works_metadata cwm ON stats.doi = cwm.doi
LEFT JOIN crossref.works cfw ON stats.doi = cfw.doi;
