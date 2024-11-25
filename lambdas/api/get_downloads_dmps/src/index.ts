import { lambdaRequestTracker } from 'pino-lambda';

 // Note that the AWS env already has the @aws-sdk installed, so if you are importing those
 // libraries, you should install them as devDependencies so that the build artifact does not include them!
 import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
 import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
 import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";

 // Import modules from ../../layers/nodeJS. These should also be included as devDependencies!
 import { deserializeDynamoItem, DMPDynamoItem } from 'dmptool-database';
 import { initializeLogger, LogLevel } from 'dmptool-logger';

const BUCKET_NAME = process.env.S3_BUCKET_NAME;
const DYNAMODB_TABLE = process.env.DYNAMODB_TABLE;

exports.handler = async (event) => {
  console.log('Received event:', JSON.stringify(event));

  // Extract user info from the Cognito Authorizer
  let userId, scopes;
  try {
      const claims = event.requestContext.authorizer.claims;
      userId = claims.sub;
      scopes = claims.scope;
  } catch (err) {
      console.error('Error extracting user identity or scope:', err);
      return {
          statusCode: 403,
          body: JSON.stringify({ message: 'Unauthorized: Missing user identity or scope' })
      };
  }

  // Ensure required scope is present
  if (!scopes || !scopes.includes('data-transfer')) {
      return {
          statusCode: 403,
          body: JSON.stringify({ message: 'Unauthorized: Missing required scope' })
      };
  }

  // Fetch user affiliations
  const affiliations = await getUserAffiliations(userId);
  if (!affiliations || affiliations.length === 0) {
      return {
          statusCode: 404,
          body: JSON.stringify({ message: 'No affiliations found for user' })
      };
  }

  // List and filter S3 objects by `dmps-` prefix
  let files;
  try {
      const listParams = {
          Bucket: BUCKET_NAME,
          Prefix: 'dmps-'
      };
      const s3Objects = await s3.listObjectsV2(listParams).promise();
      files = (s3Objects.Contents || [])
          .map(obj => obj.Key)
          .filter(key => affiliations.some(affil => key.includes(affil)));
  } catch (error) {
      console.error('Error listing S3 objects:', error);
      return {
          statusCode: 500,
          body: JSON.stringify({ message: 'Error fetching files from S3' })
      };
  }

  // Generate presigned URLs
  const presignedUrls = await Promise.all(
      files.map(async (file) => ({
          file,
          url: await generatePresignedUrl(BUCKET_NAME, file)
      }))
  );

  return {
      statusCode: 200,
      body: JSON.stringify(presignedUrls)
  };
};

const generatePresignedUrl = async (bucketName, objectKey, expiration = 3600) => {
    try {
        const url = await s3.getSignedUrlPromise('getObject', {
            Bucket: bucketName,
            Key: objectKey,
            Expires: expiration
        });
        return url;
    } catch (error) {
        console.error('Error generating presigned URL:', error);
        return null;
    }
};

const getUserAffiliations = async (userId) => {
    const params = {
        TableName: DYNAMODB_TABLE,
        KeyConditionExpression: 'userId = :userId',
        ExpressionAttributeValues: {
            ':userId': userId
        }
    };

    try {
        const result = await dynamoDB.query(params).promise();
        if (result.Items && result.Items.length > 0) {
            return result.Items[0].affiliations || [];
        } else {
            return [];
        }
    } catch (error) {
        console.error('Error querying DynamoDB:', error);
        return [];
    }
};
