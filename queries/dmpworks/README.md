# DMP Tool Related Works
The DMP Tool's related works Python package.

Requirements:
* Python 3.12
* Rust: https://www.rust-lang.org/tools/install
* Docker Engine: https://docs.docker.com/engine/install/

## Installation
Clone dmsp_api_prototype repo:
```bash
git clone git@github.com:CDLUC3/dmsp_api_prototype.git
```

Clone Polars:
```bash
git clone fix-load-json-as-string --single-branch git@github.com:jdddog/polars.git
```

Clone pyo3 Polars:
```bash
git clone --branch local-build --single-branch git@github.com:jdddog/pyo3-polars.git
```

Make a Python virtual environment:
```bash
python -m venv polars/.venv
```

Activate the Python virtual environment:
```bash
source polars/.venv/activate
```

Install Polars dependencies:
```bash
(cd polars && rustup toolchain install nightly --component miri)
(cd polars/py-polars && make requirements-all)
```

Build Polars:
```bash
(cd polars/py-polars && RUSTFLAGS="-C target-cpu=native" make build-dist-release)
```

Install dmpworks Python package dependencies:
```bash
(cd dmsp_api_prototype/queries/dmpworks && pip install -e .[dev])
```

Build and install the dmpworks Python package, including its Polars expression 
plugin:
```bash
(cd dmsp_api_prototype/queries/dmpworks && RUSTFLAGS="-C target-cpu=native" maturin develop --release)
```

## Development Environment
Start development environment:
```bash
docker compose up
```

## Tests
Running tests:
```bash
pytest
```

## Data
Data sources:
* Crossref Metadata Public Data File: https://www.crossref.org/learning/public-data-file/
* OpenAlex: https://docs.openalex.org/download-all-data/download-to-your-machine
* DataCite Public Data File: https://datafiles.datacite.org/
* ROR: https://zenodo.org/records/15132361

## Transform Source Datasets
Run the following commands to convert the source datasets into Parquet files. 
This step also performs some normalization, such as normalization of identifiers. 
Note: The full output directory must already exist before you run the commands.

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
[SQL Mesh](https://sqlmesh.readthedocs.io/en/latest/) is used to join the datasets 
and produce the works index parquet files. This functionality will be directly 
added to the dmpworks command line in the future.

Enter SQL Mesh folder:
```bash
cd dmpworks/sqlmesh
```

Make a .env file with the following exports, customise each one:
```
export SQLMESH__GATEWAYS__DUCKDB__CONNECTION__DATABASE=/path/to/db.db
export SQLMESH__VARIABLES__DATA_PATH=/path/to/parquets/
export SQLMESH__VARIABLES__EXPORT_PATH=/path/to/export/
```

Source the .env file:
```bash
source .env
```

To run the unit tests:
```bash
sqlmesh test -vv
```

To generate the export parquet files, run the plan command (select y):
```bash
sqlmesh plan -vv
```

## Create OpenSearch Indexes
Create the OpenSearch works index:
```bash
dmpworks opensearch create-index works-test works-mapping.json
```

Sync the works index export with the OpenSearch works index:
```bash
dmpworks opensearch sync-works works-test /path/to/export
```
