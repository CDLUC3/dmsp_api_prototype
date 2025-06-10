MODEL (
  name datacite_index.author_names,
  dialect duckdb,
  kind FULL
);

JINJA_QUERY_BEGIN;
SELECT
  doi,
  {{ array_agg_distinct('name') }} AS author_names,
FROM (
  -- DataCite
  SELECT doi, name -- TODO: given_name, family_name, name
  FROM datacite.works dw
  INNER JOIN datacite.works_authors dwa ON dw.doi = dwa.work_doi
  WHERE name IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, display_name AS name
  FROM datacite.works dw
  INNER JOIN openalex.works_authors owa ON dw.doi = owa.work_doi
  WHERE display_name IS NOT NULL
)
GROUP BY doi;
JINJA_END;