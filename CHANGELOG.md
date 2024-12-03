### Added
- DmpExtractor nodeJS Lambda function
- General and Database Lambda Layers for nodeJS
- Added README to the `lambda/layers/dmptool` dir explaining how nodeJS layers work
- New API Lambda, `api/get_downloads_dmps`, that generates presigned URLs to fetch DMP metadata in jsonl format

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