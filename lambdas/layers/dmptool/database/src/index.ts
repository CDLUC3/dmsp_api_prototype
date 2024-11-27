// Function to deserialize DynamoDB items. For example `{ "variableA": { "S": "value" } }` to `variableA`
// to make it easier to work with. This function is recursive and will handle "M" and "L" item types
export const deserializeDynamoItem = (item): Object => {
  const unmarshalledItem = {};

  for (const key in item) {
    const value = item[key];
    const type = Object.keys(value)[0];
    const rawValue = value[type];

    if (type === 'M') {
      // Recursively deserialize an Object (aka Map in Dynamo)
      unmarshalledItem[key] = deserializeDynamoItem(rawValue);

    } else if (type === 'L') {
      // Recursively deserialize an Array (aka List in Dynamo)
      unmarshalledItem[key] = rawValue.map((listItem) => {
        const listItemKey = Object.keys(listItem)[0];

        if (['S', 'N', 'BOOL'].includes(listItemKey)) {
          // If the list item is a primitive data type
          return listItem[listItemKey];

        } else {
          // If the list item is an Object or Array
          return deserializeDynamoItem(listItem[listItemKey]);
        }
      });

    } else {
      // Handle primitive types (e.g., S, N, BOOL)
      unmarshalledItem[key] = rawValue;
    }
  }
  return unmarshalledItem;
};

// Representation of a DynamoDB DMP record (standard DMP metadata OR an index record)
export interface DMPDynamoItem {
  PK: string;
  SK: string;
  created: string;
  modified: string;
}

export interface ClientProfile {
  PK: string;
  SK: string;
  contact: ClientProfileContact;
  description: string;
  downloadUri: string;
  homepage: string;
  name: string;
  org_access_level: string;
  redirectUri: string;
  seedingWithLiveDmpIds: boolean;
  tokenUri: string;
}

interface ClientProfileContact {
  email: string;
  name: string;
}
