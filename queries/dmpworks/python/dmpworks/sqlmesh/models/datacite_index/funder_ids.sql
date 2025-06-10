MODEL (
  name datacite_index.funder_ids,
  dialect duckdb,
  kind FULL
);

JINJA_QUERY_BEGIN;
SELECT
  doi,
  {{ array_agg_distinct('id') }} AS funder_ids,
FROM (
  -- Include ROR and Crossref Funder IDs for funder identifiers

  -- DataCite
  -- Include DataCite funder Crossref Funder IDs
  SELECT doi, funder_identifier AS id
  FROM datacite.works dw
  INNER JOIN datacite.works_funders dwf ON dw.doi = dwf.work_doi
  WHERE funder_identifier_type = 'Crossref Funder ID' AND funder_identifier IS NOT NULL

  UNION ALL

  -- Convert DataCite funder IDs to ROR IDs
  SELECT doi, ror.index.ror_id AS id
  FROM datacite.works dw
  INNER JOIN datacite.works_funders dwf ON dw.doi = dwf.work_doi
  INNER JOIN ror.index ON dwf.funder_identifier = ror.index.identifier
  WHERE ror.index.ror_id IS NOT NULL

  UNION ALL

  -- OpenAlex
  -- Get OpenAlex funder Crossref Funder IDs
  SELECT doi, ids.doi AS id
  FROM datacite.works dw
  INNER JOIN openalex.works_funders owf ON dw.doi = owf.work_doi
  INNER JOIN openalex.funders oaf ON owf.funder_id = oaf.id
  WHERE ids.doi IS NOT NULL

  UNION ALL

  -- Get ROR IDs
  -- Get OpenAlex funder ROR IDs
  SELECT doi, ids.ror AS id
  FROM datacite.works dw
  INNER JOIN openalex.works_funders owf ON dw.doi = owf.work_doi
  INNER JOIN openalex.funders oaf ON owf.funder_id = oaf.id
  WHERE ids.ror IS NOT NULL
)
GROUP BY doi;
JINJA_END;