#!/usr/bin/env bash

mkdir -p "${DEMO_DIR}/sources"/{datacite,openalex_works,crossref_metadata,openalex_funders,ror}
dmpworks demo-dataset crossref-metadata "01an7q238" "${SOURCES_DIR}/crossref_metadata/March 2025 Public Data File from Crossref" "${DEMO_DIR}/sources/crossref_metadata" --institution-name="University of California, Berkeley"
dmpworks demo-dataset datacite "01an7q238" "${SOURCES_DIR}/datacite/DataCite_Public_Data_File_2024/dois" "${DEMO_DIR}/sources/datacite" --institution-name="University of California, Berkeley"
dmpworks demo-dataset openalex-works "01an7q238" "${SOURCES_DIR}/openalex/openalex-snapshot/data/works" "${DEMO_DIR}/sources/openalex_works"

echo "Copying OpenAlex Funders"
cp -r "${SOURCES_DIR}/openalex/openalex-snapshot/data/funders/." "${DEMO_DIR}/sources/openalex_funders/"

echo "Copying ROR"
cp "${SOURCES_DIR}/ror/v1.63-2025-04-03-ror-data/v1.63-2025-04-03-ror-data_schema_v2.json" "${DEMO_DIR}/sources/ror/v1.63-2025-04-03-ror-data_schema_v2.json"
