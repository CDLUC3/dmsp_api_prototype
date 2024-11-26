import { currentDateAsString } from '..';

describe('currentDateAsString', () => {
  it('returns the date in the expected format', () => {
    const regex = /[1-9]{4}-[1-9]{2}-[1-9]{2}/;
    expect(regex.test(currentDateAsString()));
  });
});
