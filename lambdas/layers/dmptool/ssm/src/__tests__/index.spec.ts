// Need to define this all above the imports. I don't really understand why but it works
const mockSSMGetCommand = jest.fn();

// Mock the AWS SSM client
jest.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: jest.fn(() => ({
    send: mockSSMGetCommand,
  })),
  GetParameterCommand: jest.fn(),
}));

import { getSSMParameter } from '..';

describe('getSSMParameter', () => {
  it('returns the value for the specified key', async () => {
    mockSSMGetCommand.mockResolvedValueOnce({ Parameter: { Value: 'test value' } });
    expect(await getSSMParameter('TestKey')).toEqual('test value');
  });

  it(`throws an error if the specified key does not exist`, async () => {
    mockSSMGetCommand.mockResolvedValueOnce(undefined);
    await expect(getSSMParameter('TestKey')).rejects.toThrow('Specified SSM key does not exist');
  });

  it(`passes SSM errors through`, async () => {
    mockSSMGetCommand.mockImplementation(() => { throw new Error('Test SSM error') });
    await expect(getSSMParameter('TestKey')).rejects.toThrow('Test SSM error');
  });
});
