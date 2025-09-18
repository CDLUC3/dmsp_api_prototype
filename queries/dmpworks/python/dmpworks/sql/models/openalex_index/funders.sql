/*
  openalex_index.funder_ids:

  Creates a list of funders for each OpenAlex DOI. Converts OpenAlex funder IDs
  to ROR IDs. The order of the funders is maintained through WITH ORDINALITY
  and sorting on pos.
*/

MODEL (
  name openalex_index.funder_ids,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
    doi,
    array_agg(
        {
            'name': funder.funder_display_name,
            'ror': ids.ror
        } ORDER BY pos
    ) AS funders
FROM openalex_index.works_metadata AS owm
LEFT JOIN openalex.works ow ON owm.id = ow.id, UNNEST(ow.funders) WITH ORDINALITY AS item(funder, pos)
LEFT JOIN openalex.funders oaf ON funder.funder_id = oaf.id
