MODEL (
  name datacite_index.author_orcids,
  dialect duckdb,
  kind FULL
);

SELECT
  doi,
  {{ array_agg_distinct('orcid') }} AS author_orcids,
  FROM (
  -- Only use ORCID IDs for author identifiers

  -- DataCite
  SELECT doi, orcid
  FROM datacite.works dw
  INNER JOIN datacite.works_authors dwa ON dw.doi = dwa.work_doi
  WHERE orcid IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT doi, orcid
  FROM datacite.works dw
  INNER JOIN openalex.works_authors owa ON dw.doi = owa.work_doi
  WHERE orcid IS NOT NULL
)
GROUP BY doi;