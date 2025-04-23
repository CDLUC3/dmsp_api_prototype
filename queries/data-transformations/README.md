# DMP Tool Data Transformations 

## Downloading Data
Data sources:
* Crossref Metadata Public Data File: https://www.crossref.org/learning/public-data-file/
* OpenAlex: https://docs.openalex.org/download-all-data/download-to-your-machine
* DataCite Public Data File: https://datafiles.datacite.org/
* ROR: https://zenodo.org/records/15132361

## Running Scripts
Crossref Metadata:
```bash
python3 ./data_transformations/crossref_transform.py "/path/to/March 2025 Public Data File from Crossref" /path/to/transformed/crossref
```

OpenAlex Works (these are large files, so it is faster to use smaller batches):
```bash
python3 ./data_transformations/openalex_transform.py /path/to/openalex-snapshot /path/to/transformed/openalex works --max-file-processes=4 --batch-size=4
```

OpenAlex Funders:
```bash
python3 ./data_transformations/openalex_transform.py /path/to/openalex-snapshot /path/to/transformed/openalex funders
```

DataCite:
```bash
python3 ./data_transformations/datacite_transform.py /path/to/DataCite_Public_Data_File_2024 /path/to/transformed/datacite
```

ROR:
```bash
python3 ./data_transformations/ror_transform.py /path/to/ror/v1.63-2025-04-03-ror-data/v1.63-2025-04-03-ror-data_schema_v2.json /path/to/transformed/ror
```