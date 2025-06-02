# DMP Tool Related Works
The DMP Tool's related works Python package.

Requirements:
* Python 3.12
* Docker Engine: https://docs.docker.com/engine/install/

## Installation
To install locally:
```bash
pip install -e .
```

Start development environment:
```bash
docker compose up
```

## Download Data
Data sources:
* Crossref Metadata Public Data File: https://www.crossref.org/learning/public-data-file/
* OpenAlex: https://docs.openalex.org/download-all-data/download-to-your-machine
* DataCite Public Data File: https://datafiles.datacite.org/
* ROR: https://zenodo.org/records/15132361

## Transform Source Datasets
Run the following commands to convert the source datasets into Parquet files. This step also performs some normalization,
such as normalization of identifiers. Note: The full output directory must already exist before you run the commands.

Crossref Metadata:
```bash
dmpworks transform crossref-metadata "/path/to/March 2025 Public Data File from Crossref" /path/to/transformed/crossref
```

OpenAlex Works:
```bash
dmpworks transform openalex-works /path/to/openalex-snapshot /path/to/transformed/openalex_works --max-file-processes=4 --batch-size=4
```

OpenAlex Funders:
```bash
dmpworks transform openalex-funders /path/to/openalex-snapshot /path/to/transformed/openalex_funders
```

DataCite:
```bash
dmpworks transform datacite /path/to/DataCite_Public_Data_File_2024 /path/to/transformed/datacite
```

ROR:
```bash
dmpworks transform ror /path/to/ror/v1.63-2025-04-03-ror-data/v1.63-2025-04-03-ror-data_schema_v2.json /path/to/transformed/ror
```

## Create Works Index Table
TODO

## Create OpenSearch Indexes
Create the OpenSearch works index:
```bash
dmpworks opensearch create-index works-test works-mapping.json
```

Sync the hive partitioned works export with the OpenSearch works index, with an optional start date:
```bash
dmpworks opensearch sync-works works-test /path/to/export --start-date 2024-01-01
```
