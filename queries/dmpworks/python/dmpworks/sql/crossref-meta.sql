CREATE OR REPLACE TABLE crossref_meta AS
SELECT
  doi,
  LENGTH(title) AS title_length,
  LENGTH(abstract) AS abstract_length,
FROM crossref_works