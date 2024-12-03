"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
const zlib_1 = require("zlib");
const util_1 = require("util");
const pino_lambda_1 = require("pino-lambda");
// Import modules from ../../layers/nodeJS. These should also be included as devDependencies!
const dmptool_general_1 = require("dmptool-general");
const dmptool_database_1 = require("dmptool-database");
const dmptool_logger_1 = require("dmptool-logger");
const dmptool_s3_1 = require("dmptool-s3");
const gzipPromise = (0, util_1.promisify)(zlib_1.gzip);
// Environment variables
const LOG_LEVEL = process.env.LOG_LEVEL?.toLowerCase() || 'info';
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;
const FILE_PREFIX = process.env.FILE_PREFIX || "dmps";
// We want the files to be a manageable size, so set some limits
const MAX_FILE_SIZE_MB = 10;
const MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024;
// Initialize the logger
const logger = (0, dmptool_logger_1.initializeLogger)('DmpExtractorLambda', dmptool_logger_1.LogLevel[LOG_LEVEL]);
// Setup the LambdaRequestTracker for the logger
const withRequest = (0, pino_lambda_1.lambdaRequestTracker)();
// Lambda Handler - triggered by a scheduled EventBridge event
const handler = async (event, context) => {
    try {
        // Initialize the logger by setting up automatic request tracing.
        withRequest(event, context);
        // Fetch all of the registered DMPs
        const items = await (0, dmptool_database_1.getAllDMPIndexItems)();
        const sampleItem = items.length > 0 ? items[Math.round(items.length / 2)] : undefined;
        logger.debug({ sampleDMP: sampleItem }, `DMP count: ${items.length}.`);
        // Split the DMPs into manageable chunks
        const files = splitIntoFiles(items, MAX_FILE_SIZE_BYTES);
        logger.debug(undefined, `File count: ${files.length}.`);
        // Gzip and upload each file to the S3 bucket
        const tstamp = (0, dmptool_general_1.currentDateAsString)();
        await Promise.all(files.map(async (fileContent, index) => await publishFile(tstamp, fileContent, index)));
        const msg = `Uploaded ${files.length} file(s) to ${S3_BUCKET_NAME} with prefix "${FILE_PREFIX}"`;
        logger.info(undefined, msg);
        return {
            statusCode: 200,
            body: msg,
        };
    }
    catch (error) {
        logger.fatal(error, `Unable to extract the DMPs and place them into the S3 bucket.`);
        return {
            statusCode: 500,
            body: `An error occurred: ${error.message}`,
        };
    }
};
exports.handler = handler;
// Split items into multiple files based on the allowable size
const splitIntoFiles = (items, maxFileSizeBytes) => {
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
const publishFile = async (tstamp, fileContent, index) => {
    const gzippedData = await gzipPromise(fileContent.toString());
    // Set the file name (e.g. `dmps_2024-11-21_2.json.gz`)
    const fileName = `${FILE_PREFIX}_${tstamp}_${index + 1}.jsonl.gz`;
    try {
        await (0, dmptool_s3_1.putObject)(S3_BUCKET_NAME, fileName, gzippedData, 'application/json', 'gzip');
    }
    catch (err) {
        logger.fatal(err, `DMPExtractor was unable to publish file: ${fileName}`);
    }
};
