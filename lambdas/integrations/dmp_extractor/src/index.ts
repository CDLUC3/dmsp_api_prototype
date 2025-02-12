import { Context, Handler, ScheduledEvent } from 'aws-lambda';
import { gzip } from "zlib";
import { promisify } from "util";
import { lambdaRequestTracker } from 'pino-lambda';

// Import modules from ../../layers/nodeJS. These should also be included as devDependencies!
import { currentDateAsString } from 'dmptool-general';
import { getAllDMPIndexItems, DMPDynamoItem } from 'dmptool-database';
import { initializeLogger, LogLevel } from 'dmptool-logger';
import { putObject } from 'dmptool-s3';

const gzipPromise = promisify(gzip);

// Environment variables
const LOG_LEVEL = process.env.LOG_LEVEL?.toLowerCase() || 'info';
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;
const FILE_PREFIX = process.env.FILE_PREFIX || "dmps";

// We want the files to be a manageable size, so set some limits
const MAX_FILE_SIZE_MB = 100;
const MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024;

// Initialize the logger
const logger = initializeLogger('DmpExtractorLambda', LogLevel[LOG_LEVEL]);

// Setup the LambdaRequestTracker for the logger
const withRequest = lambdaRequestTracker();

// Lambda Handler - triggered by a scheduled EventBridge event
export const handler: Handler = async (event: ScheduledEvent, context: Context) => {
  try {
    // Initialize the logger by setting up automatic request tracing.
    withRequest(event, context);

    // Fetch all of the registered DMPs
    const items = await getAllDMPIndexItems();

    // Convert any boolean values (except 'featured') in the items to null
    for (const item in items) {
      Object.keys(item).forEach((key) => {
        if (key !== 'featured' && typeof item[key] === 'boolean') {
          item[key] = null;
        }
      });
    }

    // Log a sample of one of the DMPs
    // const sampleItem = items.length > 0 ? items[Math.round(items.length / 2)] : undefined;
    const sampleItem = items[0];
    logger.debug({ sampleDMP: sampleItem }, `DMP count: ${items.length}.`);

    // Split the DMPs into manageable chunks
    const files = splitIntoFiles(items, MAX_FILE_SIZE_BYTES);
    logger.debug(undefined, `File count: ${files.length}.`);

    // Gzip and upload each file to the S3 bucket
    const tstamp = currentDateAsString();
    await Promise.all(files.map( async (fileContent, index) => await publishFile(tstamp, fileContent, index)));

    const msg = `Uploaded ${files.length} file(s) to ${S3_BUCKET_NAME} with prefix "${FILE_PREFIX}"`
    logger.info(undefined, msg);

    return {
      statusCode: 200,
      body: msg,
    };
  } catch (error) {
    logger.fatal(error, `Unable to extract the DMPs and place them into the S3 bucket.`)
    return {
      statusCode: 500,
      body: `An error occurred: ${error.message}`,
    };
  }
};

// Split items into multiple files based on the allowable size
const splitIntoFiles = (items: DMPDynamoItem[], maxFileSizeBytes: number): DMPDynamoItem[][] => {
  const files = [];
  let currentFile = [];
  let currentFileSize = 0;

  for (const item of items) {
    const itemSize = Buffer.byteLength(JSON.stringify(item), "utf8");

    if (currentFileSize + itemSize > maxFileSizeBytes) {
      // If adding the current item exceeds the file size, start a new file
      files.push(currentFile);
      currentFile = [];
      currentFileSize = 0;
    }

    currentFile.push(JSON.stringify(item, null));
    currentFileSize += itemSize;
  }

  // Add the last file if it has items
  if (currentFile.length > 0) {
    files.push(currentFile);
  }
  return files.map((file) => file.join('\n'));
};

// Gzip and upload the file to the S3 bucket
const publishFile = async (tstamp: string, fileContent: DMPDynamoItem[], index: number): Promise<void> => {
  const gzippedData = await gzipPromise(fileContent.toString());

  // Set the file name (e.g. `dmps_2024-11-21_2.json.gz`)
  const fileName = `${FILE_PREFIX}_${tstamp}_${index + 1}.jsonl.gz`;

  try {
    await putObject(S3_BUCKET_NAME, fileName, gzippedData, 'application/json', 'gzip');
  } catch(err) {
    logger.fatal(err, `DMPExtractor was unable to publish file: ${fileName}`);
  }
}
