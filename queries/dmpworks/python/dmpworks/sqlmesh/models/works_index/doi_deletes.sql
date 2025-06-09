MODEL (
  name works_index.doi_deletes,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (doi, delete_date, source)
  )
);

WITH constants AS (
  SELECT CURRENT_DATE AS delete_date
),

missing_dois AS (
  SELECT doi_up.doi, doi_up.source
  FROM works_index.doi_upserts doi_up
  LEFT JOIN works_index.doi_deletes doi_del ON doi_up.doi = doi_del.doi AND doi_up.source = doi_del.source
  LEFT JOIN datacite_index.datacite_index d_idx ON doi_up.doi = d_idx.doi AND doi_up.source = 'datacite'
  LEFT JOIN openalex_index.openalex_index o_idx ON doi_up.doi = o_idx.doi AND doi_up.source = 'openalex'
  WHERE doi_del.doi IS NULL AND ((doi_up.source = 'datacite' AND d_idx.doi IS NULL) OR (doi_up.source = 'openalex' AND o_idx.doi IS NULL))
)

SELECT
  DISTINCT doi,
  constants.delete_date,
  source
FROM missing_dois
CROSS JOIN constants

