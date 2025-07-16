#!/usr/bin/env bash

mkdir -p "${DATA}/sources"/{datacite,openalex_works,crossref_metadata,openalex_funders,ror}
dmpworks demo-dataset crossref-metadata "01an7q238" "${SOURCES}/crossref_metadata/March 2025 Public Data File from Crossref" "${DATA}/sources/crossref_metadata" --institution-name="University of California, Berkeley"
dmpworks demo-dataset datacite "01an7q238" "${SOURCES}/datacite/DataCite_Public_Data_File_2024/dois" "${DATA}/sources/datacite" --institution-name="University of California, Berkeley"
dmpworks demo-dataset openalex-works "01an7q238" "${SOURCES}/openalex/openalex-snapshot/data/works" "${DATA}/sources/openalex_works"

echo "Copying OpenAlex Funders"
cp -r "${SOURCES}/openalex/openalex-snapshot/data/funders/." "${DATA}/sources/openalex_funders/"

echo "Copying ROR"
cp "${SOURCES}/ror/v1.63-2025-04-03-ror-data/v1.63-2025-04-03-ror-data_schema_v2.json" "${DATA}/sources/ror/v1.63-2025-04-03-ror-data_schema_v2.json"
