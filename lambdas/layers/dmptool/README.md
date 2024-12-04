# Lambda Layers for NodeJS

## Creating a new Layer
The easiest way to create a new layer is to create a new folder for your layer and then copy the contents of an existing layer (e.g. `general`) into your new folder.

Once the files have been copied over you will need to do the following:
- Open the `package.json` and update the contents accordingly. You must at least update the `name` and `description`, but you should review all of the values and update if necessary
- Open the `build_deploy.sh` script and change the `ZIP_NAME_SUFFIX` and `LAYER_NAME` variables at the top of the file.
- Update the contents of `src/index.ts` and `src/__tests__/index.spec.ts`

## Building and Deploying a Layer
At this time, AWS SAM does not support building Layers with esBuild, so we have to include our own `build_deploy.sh` bash script to each layer.

The bash script runs `npm install`, `tsc` to compile the Typescript, zips the resulting `dist` directory, publishes the lambda layer and then sets an SSM parameter with the ARN of the layer's version.

To run the script: `cd [layer_dir] && ./build_deploy.sh [env]`

This scriipt adds the ARN to SSM so that the AWS SAM template used to build Lambda functions that use the layer are able to reference the current version.

Once the layer version has been published, you will need to go into each function that uses the layer and update the function. (e.g. `ruby sam_build_deploy.rb [env] true true [logLevel]`).

To find out which functions use the layer, search this repositories `lambdas` directory for the name of your layer's SSM param. (e.g. `DMPToolGeneralLayerArn`)

## Running the Linter and Tests
This directory is setup to run ESLint and Jest on all of the layers at once.

**Make sure you are in the `layers/dmptool` directory!**

If this is the first time you will need to run `npm install` first.

To run the linter: `npm run lint`

To run the tests: `npm run test`

## Adding a Layer to a Lambda Function
Once the Lambda Layer has been published, we can add it to a nodeJS Lambda function.

**Make the Layer available for typescript so we can run tests and the linter**
Typescript is going to want you to import from the layer code, so we need to add the layer's TS files to the function. To do this we need to run: `npm add ../../layers/dmptool/general --save-dev` in the function's directory.
This will update the `package.json` like this:
```
  "devDependencies": {
    "dmptool-database": "file:../../layers/dmptool/database",
    "dmptool-general": "file:../../layers/dmptool/general"
  }
```

**Adding the Layer to the AWS SAM template**
To make sure that the function is able to access the layer when deployed, you will need to update the function's `template.yaml` file so that the layers are defined. The name of the parameter should match the name of the SSM parameter that gets created/updated when you build the layer (see above).

For example:

```
Parameters:
  ...

  DMPToolGeneralLayerArn:
    Type: 'String'

Resources:
  Function:
    Type: 'AWS::Serverless::Function'
    Properties:

      ...

      Layers:
        - !Ref DMPToolGeneralLayerArn

      ...
```

You will then need to update the function's `sam_build_deploy ruby` file so that it knows to fetch the SSM parameter. For example:
```
@ssm_params = %w[DMPToolGeneralLayerArn]
```

Once the layer has been added you will need to run `ruby sam_build_deploy.rb [env] true true [logLevel]` to update the function in the AWS environment so that its using the latest version of the layer.