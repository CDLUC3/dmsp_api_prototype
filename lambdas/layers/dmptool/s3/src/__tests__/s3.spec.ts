const mockS3Command = jest.fn();
const mockSignedURLCommand = jest.fn();

jest.mock("@aws-sdk/s3-request-presigner", () => ({
  getSignedUrl: mockSignedURLCommand,
}));

jest.mock('@aws-sdk/client-s3', () => ({
  S3Client: jest.fn(() => ({
    send: mockS3Command,
  })),
  GetObjectCommand: jest.fn(),
  ListObjectsV2Command: jest.fn(),
  PutObjectCommand: jest.fn(),
}));

import { listObjects, getObject, getPresignedURL, putObject } from "..";

beforeEach(() => {
  jest.resetAllMocks();
})

describe('listObjects', () => {
  it('raises errors', async () => {
    mockS3Command.mockImplementation(() => { throw new Error('Test S3 error') });

    await expect(listObjects('TestBucket', '/files')).rejects.toThrow('Test S3 error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await listObjects('', '/files')).toEqual(undefined);
  });

  it('it returns the list of objects', async () => {
    const items = [{ Key: 'Test1' }, { Key: 'Test2', Size: 12345 }];
    mockS3Command.mockResolvedValue({ Contents: items });

    expect(await listObjects('TestBucket', '/files')).toEqual([{ key: 'Test1' }, { key: 'Test2', size: 12345 }]);
  });
});

describe('getObject', () => {
  it('raises errors', async () => {
    mockS3Command.mockImplementation(() => { throw new Error('Test S3 error') });

    await expect(getObject('TestBucket', '/files')).rejects.toThrow('Test S3 error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await getObject('', '/files')).toEqual(undefined);
  });

  it('it returns undefined if no key prefix is specified', async () => {
    expect(await getObject('Test', '  ')).toEqual(undefined);
  });

  it('it returns the list of objects', async () => {
    const items = [{ key: 'Test1' }, { key: 'Test2', size: 12345 }];
    mockS3Command.mockResolvedValue(items);

    expect(await getObject('TestBucket', '/files')).toEqual(items);
  });
});

describe('putObject', () => {
  it('raises errors', async () => {
    mockS3Command.mockImplementation(() => { throw new Error('Test S3 error') });

    await expect(putObject('TestBucket', '/files', '12345')).rejects.toThrow('Test S3 error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await putObject('', '/files', '12345')).toEqual(undefined);
  });

  it('it returns undefined if no key prefix is specified', async () => {
    expect(await putObject('Test', '  ', '12345')).toEqual(undefined);
  });

  it('it returns the list of objects', async () => {
    const items = [{ key: 'Test1' }, { key: 'Test2', size: 12345 }];
    mockS3Command.mockResolvedValue(items);

    expect(await putObject('TestBucket', '/files', '12345')).toEqual(items);
  });
});

describe('getPresignedURL', () => {
  it('raises errors', async () => {
    mockSignedURLCommand.mockImplementation(() => { throw new Error('Test Signer error') });

    await expect(getPresignedURL('TestBucket', '/files')).rejects.toThrow('Test Signer error');
  });

  it('it returns undefined if no bucket is specified', async () => {
    expect(await getPresignedURL('', '/files')).toEqual(undefined);
  });

  it('it returns undefined if no key prefix is specified', async () => {
    expect(await getPresignedURL('Test', '  ')).toEqual(undefined);
  });

  it('it returns the list of objects', async () => {
    const key = '/tests/file.json';
    const presignedURL = 'http://testing.example.com/file/12345abcdefg';
    mockSignedURLCommand.mockResolvedValue(presignedURL);

    expect(await getPresignedURL('TestBucket', key)).toEqual({ fileName: key, url: presignedURL });
  });
});
