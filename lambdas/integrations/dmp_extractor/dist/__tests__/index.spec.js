"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
jest.mock('dmptool-logger');
jest.mock('@aws-sdk/client-dynamodb', () => {
    return {
        DynamoDBClient: {
            send: jest.fn(),
        }
    };
});
let mockSerializeDynamoItem;
beforeEach(() => {
    jest.resetAllMocks();
    mockSerializeDynamoItem = jest.fn();
});
afterEach(() => {
    jest.clearAllMocks();
});
describe('handler', () => {
});
describe('fetchDMPs', () => {
    beforeAll(() => {
    });
    it('Does not make multiple calls to the database if there is no ExclusiveStartKey', async () => {
    });
    it('Makes multiple calls to the database if necessary', async () => {
    });
});
describe('splitIntoFiles', () => {
});
describe('publishFile', () => {
});
