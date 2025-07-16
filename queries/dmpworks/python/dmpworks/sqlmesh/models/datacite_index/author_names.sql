/*
  datacite_index.author_names:

  Aggregates distinct author names for DataCite works found in DataCite and
  OpenAlex, grouped by DOI.
*/

MODEL (
  name datacite_index.author_names,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  @array_agg_distinct(name) AS author_names,
FROM (
  -- DataCite
  SELECT work_doi AS doi, name -- TODO: given_name, family_name, name
  FROM datacite.works_authors
  WHERE name IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, display_name AS name
  FROM datacite.works dw
  INNER JOIN openalex.works_authors owa ON dw.doi = owa.work_doi
  WHERE display_name IS NOT NULL
)
GROUP BY doi;