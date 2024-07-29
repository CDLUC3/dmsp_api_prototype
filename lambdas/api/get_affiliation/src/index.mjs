import DynamoDB from "aws-sdk/clients/dynamodb"

const dynamo = new DynamoDB.DocumentClient();
const tableName = process.env.DYNAMO_EXTERNAL_DATA_TABLE;
const resourceType = 'AFFILIATION';
const rorBaseURL = 'https://ror.org/';
const logLevel = process.env?.LOG_LEVEL || 'debug';

function responder(status, message) {
  if (logLevel === 'debug') {
    console.log({ statusCode: status, body: message });
  }
  return {
    statusCode: status,
    headers: { "Content-Type": "application/json" },
    isBase64Encoded: false,
    body: JSON.stringify(message),
  };
};

export const handler = async (event) => {
  // Extracting the affiliation_id from the path parameters
  const affiliationId = event.pathParameters && event.pathParameters.affiliation_id;
  if (!affiliationId) {
    return responder(400, { error: 'affiliation_id not found in path parameters' });
  }

  const params = {
    TableName: tableName,
    Key: { RESOURCE_TYPE: resourceType, ID: `${rorBaseURL}${affiliationId}` },
  };

  try {
    console.log(`INFO: Searching for ROR: ${affiliationId}`);
    // Fetch the Affiliation from the ExternalData Dynamo Table
    const data = await dynamo.get(params).promise();
    if (data.Item) {
      return responder(200, data.Item);
    } else {
      return responder(404, { error: 'Item not found' });
    }
  } catch (error) {
    // Handle DynamoDB specific errors
    console.log(`Fatal error: ${error.message}`);
    console.log(error.stack);
    return responder(500, { error: 'DynamoDB client error', details: error.message });
  }
};
