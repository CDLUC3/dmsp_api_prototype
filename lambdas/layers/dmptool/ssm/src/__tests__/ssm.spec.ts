const mockSSMCommand = jest.fn();

jest.mock('@aws-sdk/client-ssm', () => ({
  SSMClient: jest.fn(() => ({
    send: mockSSMCommand,
  })),
  GetParameterCommand: jest.fn(),
}));

import { getSSMParameter } from '..';

beforeEach(() => {
  jest.resetAllMocks();
})

describe('getSSMParameter', () => {
  it('raises errors', async () => {
    mockSSMCommand.mockImplementation(() => { throw new Error('Test SSM error') });

    await expect(getSSMParameter('/test/param')).rejects.toThrow('Test SSM error');
  });

  it('it returns undefined if no key is specified', async () => {
    expect(await getSSMParameter('  ')).toEqual(undefined);
  });

  it('it returns the list of objects', async () => {
    mockSSMCommand.mockResolvedValue({ Parameter: { Name: '/test/param', Value: 'Testing' } });

    expect(await getSSMParameter('/test/param')).toEqual('Testing');
  });
});