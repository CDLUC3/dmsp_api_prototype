const mockCognitoCommand = jest.fn();

jest.mock('@aws-sdk/client-cognito-identity-provider', () => ({
  CognitoIdentityProviderClient: jest.fn(() => ({
    send: mockCognitoCommand,
  })),
  DescribeUserPoolClientCommand: jest.fn(),
}));

import { verifyAPIGatewayLambdaAuthorizer } from "..";

beforeEach(() => {
  jest.resetAllMocks();

  process.env.NODE_ENV = 'testing';
})

describe('verifyAPIGatewayLambdaAuthorizer', () => {
  it('raises a Cognito error', async () => {
    const event = { requestContext: { authorizer: { claims: { sub: '00000', scope: 'dev.write' } } } };
    mockCognitoCommand.mockImplementation(() => { throw new Error('Test Cognito error') });

    await expect(verifyAPIGatewayLambdaAuthorizer('00000', event, 'write')).rejects.toThrow('Test Cognito error');
  });

  it('returns the name for the client', async () => {
    const event = { requestContext: { authorizer: { claims: { sub: '00000', scope: 'dev.write' } } } };
    mockCognitoCommand.mockResolvedValue({ UserPoolClient: { ClientName: 'Test' } });

    const response = await verifyAPIGatewayLambdaAuthorizer('00000', event, 'write');
    expect(response).toEqual({ id: '00000', name: 'Test' });
  });

  it('returns undefined if the event has no authorized claim', async () => {
    const event = { requestContext: { authorizer: { claims: {} } } };
    mockCognitoCommand.mockResolvedValue({ UserPoolClient: { ClientName: 'Test' } });

    const response = await verifyAPIGatewayLambdaAuthorizer('00000', event, 'write');
    expect(response).toEqual(undefined);
  });

  it('defaults to the read scope if none is specified', async () => {
    const event = { requestContext: { authorizer: { claims: { sub: '00000', scope: 'dev.write dev.read' } } } };
    mockCognitoCommand.mockResolvedValue({ UserPoolClient: { ClientName: 'Test' } });

    const response = await verifyAPIGatewayLambdaAuthorizer('00000', event);
    expect(response).toEqual({ id: '00000', name: 'Test' });
  });

  it('returns undefined if the event scopes are for a different env', async () => {
    const event = { requestContext: { authorizer: { claims: { sub: '00000', scope: 'prd.write' } } } };

    const response = await verifyAPIGatewayLambdaAuthorizer('00000', event, 'write');
    expect(response).toEqual(undefined);
  });

  it('returns undefined if the authorized claim does not include the specified scope', async () => {
    const event = { requestContext: { authorizer: { claims: { sub: '00000', scope: 'dev.read' } } } };

    const response = await verifyAPIGatewayLambdaAuthorizer('00000', event, 'write');
    expect(response).toEqual(undefined);
  });
});
