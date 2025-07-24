/*
  datacite_index.affiliation_names:

  Aggregates distinct affiliation names for DataCite works found in DataCite
  and OpenAlex, grouped by DOI.
*/

MODEL (
  name datacite_index.affiliation_names,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  @array_agg_distinct(name) AS affiliation_names,
FROM (
  -- DataCite
  SELECT work_doi AS doi, name
  FROM datacite.works_affiliations
  WHERE name IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, display_name AS name
  FROM datacite.works dw
  INNER JOIN openalex.works_affiliations owa ON dw.doi = owa.work_doi
  WHERE display_name IS NOT NULL
)
GROUP BY doi;