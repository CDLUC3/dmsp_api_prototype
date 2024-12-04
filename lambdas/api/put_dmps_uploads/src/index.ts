import { lambdaRequestTracker } from 'pino-lambda';
import { APIGatewayEvent, Context, Handler } from 'aws-lambda';

// Import modules from ../../layers/nodeJS. These should also be included as devDependencies!
import { getExport } from 'dmptool-cloudformation';
import { verifyAPIGatewayLambdaAuthorizer } from 'dmptool-cognito';
import { initializeLogger, LogLevel } from 'dmptool-logger';
import { getPresignedURL } from 'dmptool-s3';

const LOG_LEVEL = process.env.LOG_LEVEL?.toLowerCase() || 'info';
const COGNITO_USER_POOL_EXP_NAME = process.env.COGNITO_USER_POOL_EXP_NAME;
const S3_BUCKET_EXP_NAME = process.env.S3_BUCKET_EXP_NAME;

const MSG_UNAUTHORIZED = 'Unauthorized: Missing user identity or scope';
const MSG_FATAL = 'Unable to generate upload URL for your file at this time';
const MSG_BAD_INPUT = 'You must specify a file name in the body of your request (e.g. `{ "fileName": "test.json" }`)';

// Initialize the logger
const logger = initializeLogger('APIPutUploadsDmps', LogLevel[LOG_LEVEL]);

// Setup the LambdaRequestTracker for the logger
const withRequest = lambdaRequestTracker();

// Lambda function to create presigned URLs for the file uploads
export const handler: Handler = async (event: APIGatewayEvent, context: Context) => {
  try {
    // Initialize the logger by setting up automatic request tracing.
    withRequest(event, context);

    const bucketName = await getExport(S3_BUCKET_EXP_NAME);
    const userPoolId = await getExport(COGNITO_USER_POOL_EXP_NAME);
    let fileName: string = JSON.parse(event.body || "{}").fileName;

    if (!fileName || fileName.trim() === '') {
      return { statusCode: 400, body: JSON.stringify({ message: MSG_BAD_INPUT }) };
    }

    if (bucketName && userPoolId) {
      // Validate the caller by examining the Authorizor and ensuring it has the `data-transfer` scope
      const client = await verifyAPIGatewayLambdaAuthorizer(userPoolId, event, 'data-transfer');
      if (!client || !client.name) {
        logger.warn(client, 'Unauthorized access. Caller does not have necessary permissions')
        return { statusCode: 403, body: JSON.stringify({ message: MSG_UNAUTHORIZED }) };
      }
      logger.debug(undefined, `Generating presignedURLs for ${client.name}`);

      // Ensure that the filename is prefixed with the client name `[clientName]-`
      fileName = fileName.toLowerCase();
      fileName = fileName.startsWith(`${client.name}-`) ? fileName : `${client.name}-${fileName}`;

      // Generate the pre-signed URL
      const presignedURL = {};
      presignedURL[fileName] = await getPresignedURL(bucketName, fileName, true);
      logger.info({ fileName, presignedURL }, 'Generated a presigned upload URL');

      // Success, return the pre-signed URL
      return {
        statusCode: 200,
        body: JSON.stringify({ UploadDestination: presignedURL }),
      };
    }

    return { statusCode: 500, body: JSON.stringify({ message: `${MSG_FATAL} - configuration issue` }) };
  } catch (err) {
    logger.fatal(err, 'Unable to generate presigned URLs');
    return { statusCode: 500, body: JSON.stringify({ message: MSG_FATAL }) };
  }
}
