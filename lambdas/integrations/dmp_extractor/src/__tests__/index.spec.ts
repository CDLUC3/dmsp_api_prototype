// Need to define this all above the imports. I don't really understand why but it works
const mockGetAllDMPIndexItems = jest.fn();
const mockPutCommand = jest.fn();

// Mock the AWS DynamoDB client
jest.mock('dmptool-database', () => {
  return {
    __esModule: true,
    getAllDMPIndexItems: mockGetAllDMPIndexItems
  };
});

jest.mock('dmptool-s3', () => {
  return {
    __esModule: true,
    putObject: mockPutCommand,
  };
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

describe('Lambda Handler', () => {
  let mockContext;

  beforeEach(() => {
    jest.clearAllMocks();

    mockContext = { awsRequestId: casual.uuid }
  });

  it('should scan the DynamoDB table and upload a JSON file to S3', async () => {
    mockGetAllDMPIndexItems.mockResolvedValueOnce([{ id: '1', name: 'Item1' }]);
    mockPutCommand.mockResolvedValue({});

    const response = await handler({}, mockContext, undefined);

    const expected = {
      body: "Uploaded 1 file(s) to undefined with prefix \"dmps\"",
      statusCode: 200,
    }

    expect(mockGetAllDMPIndexItems).toHaveBeenCalledTimes(1);
    expect(mockPutCommand).toHaveBeenCalledTimes(1);
    expect(response).toEqual(expected);
  });

  it('should handle errors when DynamoDB scan fails', async () => {
    mockGetAllDMPIndexItems.mockRejectedValueOnce(new Error('DynamoDB error'));

    const response = await handler({}, mockContext, undefined);

    const expected = {
      body: "An error occurred: DynamoDB error",
      statusCode: 500,
    }

    expect(mockGetAllDMPIndexItems).toHaveBeenCalledTimes(1);
    expect(mockPutCommand).not.toHaveBeenCalled();
    expect(response).toEqual(expected);
  });

  it('should split JSON output into multiple files if size exceeds 10MB', async () => {
    const largeValue = 'x'.repeat(11 * 1024 * 1024);
    const largeItem = { id: '1', name: 'LargeItem', data: largeValue };

    mockGetAllDMPIndexItems.mockResolvedValueOnce([largeItem]);
    mockPutCommand.mockResolvedValue({});

    await handler({}, mockContext, undefined);

    expect(mockGetAllDMPIndexItems).toHaveBeenCalledTimes(1);
    expect(mockPutCommand).toHaveBeenCalledTimes(2);
  });
});
