-- Crossref: create tables and views
CREATE OR REPLACE VIEW crossref_works AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'crossref/parquets/crossref_works_[0-9]*.parquet');

CREATE OR REPLACE VIEW crossref_works_funders AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'crossref/parquets/crossref_works_funders_[0-9]*.parquet');

CREATE OR REPLACE VIEW crossref_relations AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'crossref/parquets/crossref_works_relations_[0-9]*.parquet');

-- DataCite: create tables and views
CREATE OR REPLACE VIEW datacite_works AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'datacite/parquets/datacite_works_[0-9]*.parquet');

CREATE OR REPLACE VIEW datacite_works_affiliations AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'datacite/parquets/datacite_works_affiliations_[0-9]*.parquet');

CREATE OR REPLACE VIEW datacite_works_authors AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'datacite/parquets/datacite_works_authors_[0-9]*.parquet');

CREATE OR REPLACE VIEW datacite_works_funders AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'datacite/parquets/datacite_works_funders_[0-9]*.parquet');

CREATE OR REPLACE VIEW datacite_relations AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'datacite/parquets/datacite_works_relations_[0-9]*.parquet');

-- OpenAlex: create tables and views
CREATE OR REPLACE VIEW openalex_works AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'openalex_works/parquets/openalex_works_[0-9]*.parquet');

CREATE OR REPLACE VIEW openalex_works_affiliations AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'openalex_works/parquets/openalex_works_affiliations_[0-9]*.parquet');

CREATE OR REPLACE VIEW openalex_works_authors AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'openalex_works/parquets/openalex_works_authors_[0-9]*.parquet');

CREATE OR REPLACE VIEW openalex_works_funders AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'openalex_works/parquets/openalex_works_funders_[0-9]*.parquet');

CREATE OR REPLACE VIEW openalex_funders AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'openalex_funders/parquets/openalex_funders_[0-9]*.parquet');

-- ROR: create tables and views
CREATE OR REPLACE VIEW ror_index AS
SELECT * FROM read_parquet(getvariable('DATA_PATH') || 'ror/ror.parquet');
