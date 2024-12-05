const mockGetExport = jest.fn();
const mockVerifyAPIGatewayLambdaAuthorizer = jest.fn();
const mockGetPresignedURL = jest.fn();

jest.mock('dmptool-cloudformation', () => {
  // This allows us to only mock what we want. In this case we are preserving the LogLevel enum
  const originalModule = jest.requireActual<typeof import('dmptool-cloudformation')>('dmptool-cloudformation');

  return {
    __esModule: true,
    ...originalModule,
    getExport: mockGetExport,
  };
});

jest.mock('dmptool-cognito', () => {
  return {
    __esModule: true,
    verifyAPIGatewayLambdaAuthorizer: mockVerifyAPIGatewayLambdaAuthorizer,
  };
});

jest.mock('dmptool-s3', () => {
  return {
    __esModule: true,
    getPresignedURL: mockGetPresignedURL,
  }
});

// Mock the DMPTool logger
jest.mock('dmptool-logger', () => {
  // This allows us to only mock what we want. In this case we are preserving the LogLevel enum
  const originalModule = jest.requireActual<typeof import('dmptool-logger')>('dmptool-logger');

  return {
    __esModule: true,
    ...originalModule,
    initializeLogger: jest.fn(() => ({
      fatal: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
      info: jest.fn(),
      debug: jest.fn(),
      trace: jest.fn(),
    })),
  }
});

import casual from 'casual';
import { handler } from '..'; // Ensure this is after the mocks

let mockClientId;
let mockScope;
let mockEvent;
let mockContext;

beforeEach(() => {
  jest.clearAllMocks();

  mockClientId = casual.uuid;
  mockScope = 'dev.data-transfer';

  mockEvent = {
    body: `{"fileName":"${casual.first_name}"}`,
    requestContext: {
      authorizer: {
        claims: {
          sub: mockClientId,
          token_use: 'access',
          scope: `${casual.url}/${mockScope}`,
          auth_time: casual.integer(111111, 999999),
          iss: casual.url,
          exp: 'Tue Nov 26 23:19:35 UTC 2024',
          iat: 'Tue Nov 26 23:09:35 UTC 2024',
          version: casual.integer(1, 9),
          jti: casual.uuid,
          client_id: mockClientId
        }
      }
    }
  };

  mockContext = { awsRequestId: casual.uuid };
});

describe('handler', () => {
  it('returns a 500 if an error is thrown', async () => {
    mockGetExport.mockImplementation(() => { throw new Error('Unhandled error') });
    const response = await handler(mockEvent, mockContext, undefined);

    expect(response.statusCode).toEqual(500);
    const msg = 'Unable to generate upload URL for your file at this time';
    expect(JSON.parse(response.body)).toEqual({ message: msg });
  });

  it('returns a 500 if the S3Bucket or Cognito User Pool do not exist', async () => {
    mockGetExport.mockResolvedValue(undefined);
    const response = await handler(mockEvent, mockContext, undefined);

    expect(response.statusCode).toEqual(500);
    const msg = 'Unable to generate upload URL for your file at this time - configuration issue';
    expect(JSON.parse(response.body)).toEqual({ message: msg });
  });

  it('returns a 403 if the caller is not authorized for the scope "data-transfer"', async () => {
    mockGetExport.mockResolvedValue('Test');
    mockVerifyAPIGatewayLambdaAuthorizer.mockResolvedValue(undefined);
    const response = await handler(mockEvent, mockContext, undefined);

    expect(response.statusCode).toEqual(403);
    const msg = 'Unauthorized: Missing user identity or scope';
    expect(JSON.parse(response.body)).toEqual({ message: msg });
  });

  it('returns a 400 if there is no fileName specified', async () => {
    mockGetExport.mockResolvedValue('Test');
    mockVerifyAPIGatewayLambdaAuthorizer.mockResolvedValue({ name: 'Tester' });
    mockEvent.body = null;
    const response = await handler(mockEvent, mockContext, undefined);

    expect(response.statusCode).toEqual(400);
    const msg = 'You must specify a file name in the body of your request (e.g. `{ "fileName": "test.json" }`)'
    expect(JSON.parse(response.body)).toEqual({ message: msg });
  });

  it('returns a 200 with the pre-signed URL', async () => {
    const mockUrl = 'http://example.com/test/1';
    mockGetExport.mockResolvedValue('Test');
    mockVerifyAPIGatewayLambdaAuthorizer.mockResolvedValue({ name: 'Tester' });
    mockGetPresignedURL.mockResolvedValueOnce(mockUrl);
    const response = await handler(mockEvent, mockContext, undefined);

    expect(response.statusCode).toEqual(200);
    const expected = {};
    expected[`Tester-${JSON.parse(mockEvent.body).fileName.toLowerCase()}`] = mockUrl;
    expect(JSON.parse(response.body)).toEqual({ UploadDestination: expected });
  });
});
