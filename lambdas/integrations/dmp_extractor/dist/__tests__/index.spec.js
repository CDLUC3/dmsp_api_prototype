"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// Need to define this all above the imports. I don't really understand why but it works
const mockDynamoDBSend = jest.fn();
const mockS3Send = jest.fn();
// Mock the AWS DynamoDB client
jest.mock('@aws-sdk/client-dynamodb', () => ({
    DynamoDBClient: jest.fn(() => ({
        send: mockDynamoDBSend,
    })),
    ScanCommand: jest.fn(),
}));
// Mock the AWS S3 client
jest.mock('@aws-sdk/client-s3', () => ({
    S3Client: jest.fn(() => ({
        send: mockS3Send,
    })),
    PutObjectCommand: jest.fn(),
}));
// Mock the DMPTool logger
jest.mock('dmptool-logger', () => {
    // This allows us to only mock what we want. In this case we are preserving the LogLevel enum
    const originalModule = jest.requireActual('dmptool-logger');
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
    };
});
const casual_1 = __importDefault(require("casual"));
const __1 = require(".."); // Ensure this is after the mocks
describe('Lambda Handler', () => {
    let mockContext;
    beforeEach(() => {
        jest.clearAllMocks();
        mockContext = { awsRequestId: casual_1.default.uuid };
    });
    it('should scan the DynamoDB table and upload a JSON file to S3', async () => {
        // Mock DynamoDB Scan responses
        mockDynamoDBSend
            .mockResolvedValueOnce({
            Items: [{ id: { S: '1' }, name: { S: 'Item1' } }],
            LastEvaluatedKey: 'lastKey1',
        })
            .mockResolvedValueOnce({
            Items: [{ id: { S: '2' }, name: { S: 'Item2' } }],
            LastEvaluatedKey: null,
        });
        // Mock S3 upload
        mockS3Send.mockResolvedValue({});
        // Invoke the handler
        const response = await (0, __1.handler)({}, mockContext, undefined);
        const expected = {
            body: "Uploaded 1 file(s) to undefined with prefix \"dmps\"",
            statusCode: 200,
        };
        // Assertions
        expect(mockDynamoDBSend).toHaveBeenCalledTimes(2);
        expect(mockS3Send).toHaveBeenCalledTimes(1);
        expect(response).toEqual(expected);
    });
    it('should handle errors when DynamoDB scan fails', async () => {
        // Mock DynamoDB Scan failure
        mockDynamoDBSend.mockRejectedValueOnce(new Error('DynamoDB error'));
        // Invoke the handler
        const response = await (0, __1.handler)({}, mockContext, undefined);
        const expected = {
            body: "An error occurred: DynamoDB error",
            statusCode: 500,
        };
        // Assertions
        expect(mockDynamoDBSend).toHaveBeenCalledTimes(1);
        expect(mockS3Send).not.toHaveBeenCalled();
        expect(response).toEqual(expected);
    });
    it('should split JSON output into multiple files if size exceeds 10MB', async () => {
        const largeValue = 'x'.repeat(11 * 1024 * 1024);
        const largeItem = { id: { S: '1' }, name: { S: 'LargeItem' }, data: { S: largeValue } };
        // Mock DynamoDB Scan responses
        mockDynamoDBSend.mockResolvedValueOnce({
            Items: [largeItem],
            LastEvaluatedKey: null,
        });
        mockS3Send.mockResolvedValue({});
        await (0, __1.handler)({}, mockContext, undefined);
        // Assertions
        expect(mockDynamoDBSend).toHaveBeenCalledTimes(1);
        expect(mockS3Send).toHaveBeenCalledTimes(2);
    });
});
