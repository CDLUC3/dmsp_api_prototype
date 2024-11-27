import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";

// Create an SSM client
const client = new SSMClient();

const ENV = process.env.NODE_ENV === 'production' ? 'prd' : (process.env.NODE_ENV === 'staging' ? 'stg' : 'dev');
const KEY_PREFIX = `/uc3/dmp/tool/${ENV}/`;

// SSM error message
export class DMPToolSSMNotFoundError extends Error {
  constructor(message = 'Specified SSM key does not exist') {
    super(message);
    this.name = "ValidationError";
  }
}

// Function to retrieve a variable from the SSM Parameter store
export const getSSMParameter = async (key: string): Promise<string> => {
  const command = new GetParameterCommand({ Name: `${KEY_PREFIX}${key}`, WithDecryption: true });
  const response = await client.send(command);

  // If the response was empty then throw a not found error
  if (!response || !response.Parameter.Value) {
    throw new DMPToolSSMNotFoundError();
  }

  return response.Parameter.Value;
};
