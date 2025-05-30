COPY (
  SELECT
    *,
--    EXTRACT(year FROM updated_date) AS year,
--    EXTRACT(month FROM updated_date) AS month,
--    EXTRACT(day FROM updated_date) AS day,
    'datacite' AS source
  FROM datacite_index

  UNION ALL

  SELECT
    *,
--    EXTRACT(year FROM updated_date) AS year,
--    EXTRACT(month FROM updated_date) AS month,
--    EXTRACT(day FROM updated_date) AS day,
    'openalex' AS source
  FROM openalex_index
) TO 'export' (FORMAT PARQUET, FILE_SIZE_BYTES '100MB');