"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAllDMPIndexItems = void 0;
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
// Get the table names from the ENV
const INDEX_TABLE_NAME = process.env.INDEX_TABLE_NAME;
// Initialize AWS SDK clients (outside the handler function)
const dynamoDBClient = new client_dynamodb_1.DynamoDBClient({});
// Function to deserialize DynamoDB items. For example `{ "variableA": { "S": "value" } }` to `variableA`
// to make it easier to work with. This function is recursive and will handle "M" and "L" item types
const deserializeDynamoItem = (item) => {
    const unmarshalledItem = {};
    for (const key in item) {
        const value = item[key];
        const type = Object.keys(value)[0];
        const rawValue = value[type];
        if (type === 'M') {
            // Recursively deserialize an Object (aka Map in Dynamo)
            unmarshalledItem[key] = deserializeDynamoItem(rawValue);
        }
        else if (type === 'L') {
            // Recursively deserialize an Array (aka List in Dynamo)
            unmarshalledItem[key] = rawValue.map((listItem) => {
                const listItemKey = Object.keys(listItem)[0];
                if (['S', 'N', 'BOOL'].includes(listItemKey)) {
                    // If the list item is a primitive data type
                    return listItem[listItemKey];
                }
                else {
                    // If the list item is an Object or Array
                    return deserializeDynamoItem(listItem[listItemKey]);
                }
            });
        }
        else if (type === 'NULL') {
            // Handle NULL type
            unmarshalledItem[key] = null;
        }
        else {
            // Handle primitive types (e.g., S, N, BOOL)
            unmarshalledItem[key] = rawValue;
        }
    }
    return unmarshalledItem;
};
// Fetch all of the DMP metadata index records
const getAllDMPIndexItems = async () => {
    const params = {
        FilterExpression: "SK = :sk",
        ExpressionAttributeValues: { ":sk": { S: "METADATA" } },
    };
    return await scanTable(INDEX_TABLE_NAME, params);
};
exports.getAllDMPIndexItems = getAllDMPIndexItems;
// Scan the Index table for the specified criteria
const scanTable = async (table, params) => {
    let items = [];
    let lastEvaluatedKey;
    // Query the DynamoDB index table for all DMP metadata (with pagination)
    do {
        const command = new client_dynamodb_1.ScanCommand({
            TableName: table,
            ExclusiveStartKey: lastEvaluatedKey,
            ...params
        });
        const response = await dynamoDBClient.send(command);
        // Collect items and update the pagination key
        items = items.concat(response.Items || []);
        // LastEvaluatedKey is the position of the end cursor from the query that was just run
        // when it is undefined, then the query reached the end of the results.
        lastEvaluatedKey = response.LastEvaluatedKey;
    } while (lastEvaluatedKey);
    // Deserialize and split items into multiple files if necessary
    return items.map((item) => deserializeDynamoItem(item));
};
