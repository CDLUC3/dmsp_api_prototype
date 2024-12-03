import {
  CloudFormationClient,
  ListExportsCommand,
  Export
} from "@aws-sdk/client-cloudformation";

// If you want to make a CloudFormation stack output available for to this Lambda Layer, you need to
// ensure that your CloudFormation template "exports" the output variable. Note that you should try
// to make sure that the "Name" you use for your export is namespaced to make it unique!
//
// For example:
//    Outputs:
//      UserPoolArn:
//        Value: !GetAtt UserPool.Arn
//          Export:
//            Name: !Sub 'my-namespace-CognitoUserPoolArn'
//
// You would then import the `getExport` function below into your Lambda function and pass in the
// export name. For example: `const val = await getExport("my-namespace-CognitoUserPoolArn");`

const cfClient = new CloudFormationClient({});

// Example of a CloudFormation stack export:
//   {
//     ExportingStackId: 'arn:aws:cloudformation:us-west-2:00000000:stack/my-stack-name/unique-id',
//     Name: 'DynamoDBTable1234',
//     Value: 'arn:aws:dynamodb:us-west-2:00000000:table/my-dynamo-table-19RCAN1IAZXQ4'
//   }
const cfExports: Export[] = [];

// Collect all of the CloudFormation exported outputs
const loadExports = async (): Promise<void> => {
  let nextToken;

  // if the cfExports have already been collected
  if (!cfExports || !Array.isArray(cfExports) || cfExports.length === 0) {
    do {
      const command = new ListExportsCommand({ NextToken: nextToken });
      const response = await cfClient.send(command);
      for (const exp of response.Exports) {
        cfExports.push(exp);
      }
      nextToken = response.NextToken;
    } while(nextToken);
  }
}

// Fetch the value for the specified CloudFormation stack export
export const getExport = async (name: string): Promise<string> => {
  await loadExports();
  const response = cfExports.find((exp) => exp.Name.toLowerCase().trim() === name.toLowerCase().trim());
  return response ? response.Value : undefined;
}
