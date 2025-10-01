### Added
- DmpExtractor nodeJS Lambda function
- General and Database Lambda Layers for nodeJS
- Added README to the `lambda/layers/dmptool` dir explaining how nodeJS layers work
- New API Lambda, `lambda/api/get_dmps_downloads`, that generates presigned URLs to fetch DMP metadata in jsonl format
- New API Lambda, `lambda/api/put_dmps_uploads`, that generates presigned URLs to put files into the S3 bucket

## v1.4.3
### Added
- New harvester `lambdas/harvesters/ror/` has 2 new Lambda functions.
  - 1) Downloader is scheduled and gets kicked off on the first of each month. It queries Zenodo for the latest ROR snapshot. It stashes the snapshot in S3, then unzips it and determines how many records there are. It then sends a message to an SNS topic for every 50,000 records. Those messages invoke the 2nd new Lambda
  - 2) Processor is kicked off by an SNS message. It fetches the latest ROR ZIP from S3, unzips it and then processes the records it is instructed to (e.g. 50,001 - 100,000). Processing means that it adds/updates each record into the new ExternalData DynamoDB Table
- New indexer `lambdas/indexer/external_data/` that monitors the DynamoDB Table stream for the new ExternalData table that houses our local copy of data from systems like ROR
- New script `scripts/start_ror_harvest.rb` a new helper script that can be used to manually start the ROR downloader Lambda function.

## v1.4.2
### Added
- Initial move of files from dmsp_aws_prototype repo into this repo
- Added README documentation
- Added diagram

## Related Works
### Added
- Refactor related works pipeline to use objects rather than lists of strings for authors, institutions, funders and awards.
- Dockerfile to be able to run dmpworks in AWS Batch.
- Commands for running DataCite, OpenAlex Funders and OpenAlex Works pipelines in AWS Batch.
- Migrated from argparse to cyclopts for building the CLI.
- Unit tests to check that the CLI is functioning correctly.
- Commands to run SQLMesh test and plan programmatically.
- Updated README with additional documentation.
- Command line interface to create a subset of each dataset.

### Bug Fixes
- Corrections to Polars transforms and SQL.