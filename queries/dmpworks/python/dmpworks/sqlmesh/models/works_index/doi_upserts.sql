MODEL (
  name works_index.doi_upserts,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (doi, upsert_date, source)
  )
);

SELECT DISTINCT doi, upsert_date, source
FROM (
  SELECT doi, updated_date AS upsert_date, 'datacite' AS source
  FROM datacite_index.datacite_index;

  UNION ALL

  SELECT doi, updated_date AS upsert_date, 'openalex' AS source
  FROM openalex_index.openalex_index;
)

-- Cleanup old records
DELETE FROM works_index.doi_upserts
WHERE (doi, upsert_date, source) NOT IN (
  SELECT doi, upsert_date, source
  FROM (
    SELECT
      doi,
      upsert_date,
      source,
      ROW_NUMBER() OVER (PARTITION BY doi ORDER BY upsert_date DESC) AS row_num
    FROM works_index.doi_upserts
  )
  WHERE row_num <= @VAR('doi_upserts_n_records_keep', 3) -- Default to 3
);