const mockCFCommand = jest.fn();

jest.mock('@aws-sdk/client-cloudformation', () => ({
  CloudFormationClient: jest.fn(() => ({
    send: mockCFCommand,
  })),
  ListExportsCommand: jest.fn(),
}));

import { getExport } from "..";

beforeEach(() => {
  jest.resetAllMocks();
})

describe('getExport', () => {
  it('raises a CloudFormation error', async () => {
    mockCFCommand.mockResolvedValue({ Exports: []});
    mockCFCommand.mockImplementation(() => { throw new Error('Test CloudFormation error') });
    await expect(getExport('BadTest')).rejects.toThrow('Test CloudFormation error');
  });

  it('it loads all of the available exports once', async () => {
    mockCFCommand.mockResolvedValue({ Exports: [{ Name: 'Test', Value: 'Passed' }]});

    await getExport('Test');
    await getExport('test');
    expect(mockCFCommand).toHaveBeenCalledTimes(1)
  });

  it('returns the value for the export', async () => {
    mockCFCommand.mockResolvedValue({ Exports: [{ Name: 'Test', Value: 'Passed' }]});

    const response = await getExport('Test');
    expect(response).toEqual('Passed');
  });

  it('returns undefined if the export is missing', async () => {
    mockCFCommand.mockResolvedValue({ Exports: [{ Name: 'Test', Value: 'Passed' }]});

    const response = await getExport('BadTest');
    expect(response).toEqual(undefined);
  });
});
