import { lambdaRequestTracker } from 'pino-lambda';
import { APIGatewayEvent, Context, Handler } from 'aws-lambda';

// Import modules from ../../layers/nodeJS. These should also be included as devDependencies!
import { getExport } from 'dmptool-cloudformation';
import { verifyAPIGatewayLambdaAuthorizer } from 'dmptool-cognito';
import { initializeLogger, LogLevel } from 'dmptool-logger';
import { getPresignedURL, listObjects, DMPToolPresignedURLOutput } from 'dmptool-s3';

const LOG_LEVEL = process.env.LOG_LEVEL?.toLowerCase() || 'info';
const COGNITO_USER_POOL_EXP_NAME = process.env.COGNITO_USER_POOL_EXP_NAME;
const S3_BUCKET_EXP_NAME = process.env.S3_BUCKET_EXP_NAME;

const MSG_UNAUTHORIZED = 'Unauthorized: Missing user identiity or scope';
const MSG_FATAL = 'Unable to generate download URLs for your DMP metadata at this time';

// Initialize the logger
const logger = initializeLogger('APIGetDownloadsDmps', LogLevel[LOG_LEVEL]);

// Setup the LambdaRequestTracker for the logger
const withRequest = lambdaRequestTracker();

// Lambda function to fetch presigned URLs for the DMP download files
export const handler: Handler = async (event: APIGatewayEvent, context: Context) => {
  try {
    // Initialize the logger by setting up automatic request tracing.
    withRequest(event, context);

    const bucketName = await getExport(S3_BUCKET_EXP_NAME);
    const userPoolId = await getExport(COGNITO_USER_POOL_EXP_NAME);

    if (bucketName && userPoolId) {
      // Validate the caller by examining the Authorizor and ensuring it has the `data-transfer` scope
      const client = await verifyAPIGatewayLambdaAuthorizer(userPoolId, event, 'data-transfer');
      if (!client || !client.name) {
        logger.warn(client, 'Unauthorized access. Caller does not have necessary permissions')
        return { statusCode: 403, body: JSON.stringify({ message: MSG_UNAUTHORIZED }) };
      }
      logger.debug(undefined, `Generating presignedURLs for ${client.name}`);

      // List and filter S3 objects by `[clientName]-dmps-` prefix
      const presignedUrls: DMPToolPresignedURLOutput[] = [];
      const s3Objects = await listObjects(bucketName, `${client.name}-dmps`);

      // If there are no files available for download then return a 404
      if (!s3Objects || !Array.isArray(s3Objects)) {
        logger.info(undefined, 'No DMP metadata files available');
        return { statusCode: 404, body: JSON.stringify({ message: 'No files found'}) };
      }
      logger.debug({ s3Objects }, `Detected ${s3Objects.length} DMP metadata files`);

      // Generate a presigned URL for each file in the S3 bucket
      for (const obj of s3Objects) {
        presignedUrls.push(await getPresignedURL(bucketName, obj.key));
      }
      logger.info({ presignedUrls }, `Generated ${presignedUrls.length} presigned URLs`);

      // Success, return the pre-signed URL(s)
      return {
        statusCode: 200,
        body: JSON.stringify({ DMPMetadataFiles: presignedUrls }),
      };
    }

    return { statusCode: 500, body: JSON.stringify({ message: `${MSG_FATAL} - configuration issue` }) };
  } catch (err) {
    logger.fatal(err, 'Unable to generate presigned URLs');
    return { statusCode: 500, body: JSON.stringify({ message: MSG_FATAL }) };
  }
}
