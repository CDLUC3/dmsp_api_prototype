import { APIGatewayEvent } from 'aws-lambda';
import {
  CognitoIdentityProviderClient,
  DescribeUserPoolClientCommand,
  DescribeUserPoolClientCommandOutput,
} from '@aws-sdk/client-cognito-identity-provider';

const ENV = process.env.NODE_ENV === 'production' ? 'prd' : (process.env.NODE_ENV === 'staging' ? 'stg' : 'dev');

const cognitoClient = new CognitoIdentityProviderClient({});

export interface CognitoClient {
  id: string;
  name: string;
}

// Fetch the UserPool Client's name
const getClientName = async (userPoolId: string, clientId: string): Promise<string> => {
  const params = { UserPoolId: userPoolId, ClientId: clientId };

  const command = new DescribeUserPoolClientCommand(params);
  const response: DescribeUserPoolClientCommandOutput = await cognitoClient.send(command);

  return (response && response.UserPoolClient) ? response.UserPoolClient.ClientName : undefined;
}

// Returns the Cognito Client based on the Authorizer information in the API event
export const verifyAPIGatewayLambdaAuthorizer = async (
  userPoolId: string,
  event: APIGatewayEvent,
  expectedScope = 'read',
): Promise<CognitoClient> => {
  const claims = event?.requestContext?.authorizer?.claims;
  const clientId = claims?.sub;
  const scopes = Array.isArray(claims?.scope) ? claims?.scope.join(' ') : claims?.scope;

  // Make sure the caller was Authorized and has the expected scope
  if (clientId && scopes && scopes.includes(`${ENV}.${expectedScope}`)) {
    // Fetch the Client's name from Cognito
    const clientName = await getClientName(userPoolId, clientId);

    if (clientName) {
      return { id: clientId, name: clientName };
    }
  }
  return undefined;
}
