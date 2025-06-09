MODEL (
  name datacite_index.affiliation_names,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  {{ array_agg_distinct('name') }} AS affiliation_names,
FROM (
  -- DataCite
  SELECT doi, name
  FROM datacite.works dw
  INNER JOIN datacite.works_affiliations dwa ON dw.doi = dwa.work_doi
  WHERE name IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, display_name AS name
  FROM datacite.works dw
  INNER JOIN openalex.works_affiliations owa ON dw.doi = owa.work_doi
  WHERE display_name IS NOT NULL
)
GROUP BY doi;