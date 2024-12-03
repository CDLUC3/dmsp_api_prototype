"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.deserializeDynamoItem = void 0;
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
            unmarshalledItem[key] = (0, exports.deserializeDynamoItem)(rawValue);
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
                    return (0, exports.deserializeDynamoItem)(listItem[listItemKey]);
                }
            });
        }
        else {
            // Handle primitive types (e.g., S, N, BOOL)
            unmarshalledItem[key] = rawValue;
        }
    }
    return unmarshalledItem;
};
exports.deserializeDynamoItem = deserializeDynamoItem;
