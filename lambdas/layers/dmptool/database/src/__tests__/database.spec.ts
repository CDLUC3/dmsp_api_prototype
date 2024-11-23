import { deserializeDynamoItem } from "..";

describe('deserializeDynamoItem', () => {
  it('can handle a String', () => {
    const item = { variableA: { S: 'valueA' } };
    expect(deserializeDynamoItem(item)).toEqual({ variableA: 'valueA' });
  });

  it('can handle an Object', () => {
    const item = { variableA: { M: { subVariableB: { S: 'valueA' } } } };
    expect(deserializeDynamoItem(item)).toEqual({ variableA: { subVariableB: 'valueA' } });
  });

  it('can handle an Array', () => {
    const item = { variableA: { L: [{ S: 'valueA' }, { S: 'valueB'}] } };
    expect(deserializeDynamoItem(item)).toEqual({ variableA: ['valueA', 'valueB'] });
  });

  it('can handle complex nesting', () => {
    const item = {
      variableA: {
        L: [{
          M: {
            subVariableB: {
              M: {
                subSubVariableC: { S: 'valueC' },
                subSubVariableD: { S: 'valueD' }
              }
            },
            subVariableI: { S: 'valueI' }
          }
        }]
      },
      variableE: {
        M: {
          subVariableF: { S: 'valueF' },
          subVariableG: { S: 'valueG' }
        }
      },
      variableH: { S: 'valueH' }
    };

    const expected = {
      variableA: [{
        subVariableB: { subSubVariableC: 'valueC', subSubVariableD: 'valueD' },
        subVariableI: 'valueI'
      }],
      variableE: {
        subVariableF: 'valueF',
        subVariableG: 'valueG'
      },
      variableH: 'valueH'
    };
    expect(deserializeDynamoItem(item)).toEqual(expected);
  });
});
