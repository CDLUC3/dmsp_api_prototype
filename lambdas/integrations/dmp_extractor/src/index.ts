import { Context, Handler, ScheduledEvent } from 'aws-lambda';
import { gzip } from "zlib";
import { promisify } from "util";
import { lambdaRequestTracker } from 'pino-lambda';

// Note that the AWS env already has the @aws-sdk installed, so if you are importing those
// libraries, you should install them as devDependencies so that the build artifact does not include them!
import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

// Import modules from ../../layers/nodeJS. These should also be included as devDependencies!
import { currentDateAsString } from 'dmptool-general';
import { deserializeDynamoItem, DMPDynamoItem } from 'dmptool-database';
import { initializeLogger, LogLevel } from 'dmptool-logger';

const gzipPromise = promisify(gzip);

// Environment variables
const LOG_LEVEL = process.env.LOG_LEVEL?.toLowerCase() || 'info';
const TABLE_NAME = process.env.TABLE_NAME;
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;
const FILE_PREFIX = process.env.FILE_PREFIX || "dmps";

// Initialize AWS SDK clients (outside the handler function)
const dynamoDBClient = new DynamoDBClient({});
const s3Client = new S3Client({});

// We want the files to be a manageable size, so set some limits
const MAX_FILE_SIZE_MB = 10;
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
    const items = await fetchDMPs();
    const sampleItem = items.length > 0 ? items[Math.round(items.length / 2)] : undefined;
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

// Fetch all DMP metadata from the DyanmoBD Index Table
const fetchDMPs = async (): Promise<DMPDynamoItem[]> => {
  let items = [];
  let lastEvaluatedKey;

  // Query the DynamoDB index table for all DMP metadata (with pagination)
  do {
    const params = {
      TableName: TABLE_NAME,
      ExclusiveStartKey: lastEvaluatedKey,
      FilterExpression: "SK = :sk",
      ExpressionAttributeValues: { ":sk": { S: "METADATA" } },
    };
    const command = new ScanCommand(params);
    const response = await dynamoDBClient.send(command);

    // Collect items and update the pagination key
    items = items.concat(response.Items || []);
    // LastEvaluatedKey is the position of the end cursor from the query that was just run
    // when it is undefined, then the query reached the end of the results.
    lastEvaluatedKey = response.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  // Deserialize and split items into multiple files if necessary
  return items.map((item) => deserializeDynamoItem(item));
}

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
  return files;
};

// Gzip and upload the file to the S3 bucket
const publishFile = async (tstamp: string, fileContent: DMPDynamoItem[], index: number): Promise<void> => {
  const gzippedData = await gzipPromise(fileContent.toString());

  // Set the file name (e.g. `dmps_2024-11-21_2.json.gz`)
  const fileName = `${FILE_PREFIX}_${tstamp}_${index + 1}.jsonl.gz`;

  const s3Params = {
    Bucket: S3_BUCKET_NAME,
    Key: fileName,
    Body: gzippedData,
    ContentType: "application/json",
    ContentEncoding: "gzip",
  };

  await s3Client.send(new PutObjectCommand(s3Params));
}
