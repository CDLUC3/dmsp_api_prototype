import DynamoDB from "aws-sdk/clients/dynamodb"

const dynamo = new DynamoDB.DocumentClient();
const tableName = process.env.DYNAMO_INDEX_TABLE;
const resourceType = 'AFFILIATION';
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
  // Extracting the search term and funderOnly flag from the path parameters
  let searchTerm = event.queryStringParameters && event.queryStringParameters.search;
  const funderOnly = event.queryStringParameters && event.queryStringParameters.funderOnly;

  if (!searchTerm || searchTerm.length < 3) {
    return responder(400, { error: 'search term must be greater than 3 characters' });
  }

  // Transform the searchTerm into lowercase and only alpha numeric
  searchTerm = searchTerm.toLowerCase().trim().replace(/\s/g, '');

  let filterExpression = 'contains(searchName, :term)';
  if (funderOnly && funderOnly.toString().toLowerCase().trim() === 'true') {
    filterExpression = 'contains(searchName, :term) and attribute_exists(fundref_url)';
  }

  const params = {
    TableName: tableName,
    KeyConditionExpression: "PK = :pk",
    FilterExpression: filterExpression,
    ExpressionAttributeValues: {
      ":pk": resourceType,
      ":term": searchTerm,
    },
  };

  try {
    console.log(`Searching for affiliations matching: '${searchTerm}' - (funderOnly? ${funderOnly})`);
    // Fetch the Affiliation from the ExternalData Dynamo Table
    const data = await dynamo.query(params).promise();
    if (data.Items) {
      return responder(200, data.Items);
    } else {
      return responder(404, { error: 'No affiliations found' });
    }
  } catch (error) {
    // Handle DynamoDB specific errors
    console.log(`Fatal error: ${error.message}`);
    console.log(error.stack);
    return responder(500, { error: 'DynamoDB client error', details: error.message });
  }
};
