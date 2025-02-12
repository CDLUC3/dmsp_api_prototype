# DMSP Prototype - API

This repository manages all of the Lambda functions that support the DMPHub API and the harvesters that search across various external data sources to find related works.

This code base uses the Amazon Web Services (AWS) [Serverless Application Model (SAM) system](https://aws.amazon.com/serverless/sam/) to build and deploy the Lambda functions.

Please note that you must have an AWS account to deploy the system and that doing sowill create resources in your account that may incur charges!

All of these Lambdas rely on AWS resources that are managed within the [DMSP Prototype - Infrastructure repository](https://github.com/CDLUC3/dmsp_aws_prototype). See the following diagram for details.

<img src="docs/Lambda-Use-Diagram.png?raw=true">

## Directory layout

Directory structure:
```
  docs                 # Documentation and diagrams
  |
  gems                 # Ruby gem files used by the Lambda functions
  |
  lambdas
  |     |
  |     api            # Lambdas that are kicked off by API requests
  |     |
  |     harvesters     # Lambdas that are scheduled and collect data from other systems
  |     |
  |     indexers       # Lambdas that are kicked off by Dynamo stream to sync indices
  |     |
  |     layers         # Lambda Layers that pull together shared code (e.g. gems)
  |     |
  |     |-- dmptool    # Layers written in NodeJS (all other subdirectories are Ruby based)
  |     |
  |     utilities      # Lambdas that are kicked off by calls to EventBridge
  |
  landing_page         # The React JS DMP landing page code. Hosted on CloudFront
  |
  scripts              # Helper scripts to perform various tasks
  |
  swagger              # The OpenAPI UI (aka Swagger)
  |
  test                 # Integration tests
```

## Installation and Setup

You first need to build the following AWS resources from the [DMSP Prototype - Infrastructure repository](https://github.com/CDLUC3/dmsp_aws_prototype) (see the diagram above for reference)
- [env]/global/cert.yaml
- [env]/global/route53.yaml
- [env]/regional/s3.yaml
- [env]/regional/sqs.yaml
- [env]/global/cloudfront.yaml - Hosts the DMP Landing Page code and Swagger-ui code
- [env]/regional/cognito.yaml
- [env]/regional/dynamo.yaml - Be sure to run the scripts/seed_dynamo.sh
- [env]/regional/dynamo-index-table.yaml
- [env]/regional/eventbridge.yaml
- [env]/regional/lambda-iam.yaml

Once those resources are in place, you can build and deploy the resources in this repository.

Make sure you have AWS credentials installed on your machine before running these commands.

Possible log levels for the scripts below: `debug, info, warn, error`

### Deploy the API
To build out the API, you should navigate to the `lambdas/layers/api` directory and run the following script `ruby sam_build_deploy.rb [env] true true [log_level]`.

This will build and deploy a new LambdaLayer. Once it has been deployed, it will begin to build and deploy the API's Lambda functions.

### Deploy the Indexer, Harvester and Utility functions
To build out the Lamba functions that are triggered by DynamoDB table streams and EventBridge rules, you should navigate to the `lambdas/layers/baseline` directory and run the following script `ruby sam_build_deploy.rb [env] true true [log_level]`.

This will build and deploy a new LambdaLayer. Once it has been deployed, it will begin to build and deploy each of the indexer, harvester and utitlity Lambda functions one at a time.

## Making and deploying changes

Each of the components of this repository, except the API, can be built and deployed individually.

You should checkout a new branch any time you make changes even if it is only to update the gem dependencies!

**Modifying the Gems**
If you need to modify one of the gems in the `gems/uc3-dmp-[name]/` directory or you want to add a new gem, you will need to do the following:
- Make your changes to the code
- Bump the version number in `lib/uc3-dmp-[name]/version.rb`
- Remove the existing gem file from the directory if applicable: `rm *.gem`
- Build the gem: `gem build [name].gemspec`
- Publish the gem to RubyGems: `gem push [name]-[version].gem`

If you added a new gem you will need to go into the appropriate `lambdas/layers/` directory (`api` to allow the API lambdas to use your gem and/or `baseline` to give the indexer, harvester and utility lambdas access to your gem) and then add your gem to the `Gemfile`. Once it is in the Gemfile you need to run `bundle install`

If you simply updated a gem you should go to the appropriate Lambda Layer directory(ies) and run `bundle update`

Once you have updated a Lambda Layer, you will need to run `ruby sam_build_deploy.rb [env] true true [log_level]` to deploy the updated Layer and all of the relevant Lambda functions.

**Adding an API Endpoint**
Add a new directory to the `lambdas/api/` directory. You should name your directory according to the HTTP verb that will be used to call it and the path. For example: `get_dmps`, `post_dmp_funding`, etc.

It is helpful to copy an existing function's code to use as your starting point because it pulls in the Lambda Layer and uses some of our standardized gems to do things like setup your logger, format the response, etc.

Once you have built your lambda you should go up a directory level to `lambdas/api/` and run `ruby sam_build_deploy.rb [env] true true [log_level]`.

Once your code has been deployed, we recommend logging into the AWS console (dev account) and navigate to your new Lambda function. From there you can run tests and make small modifications until you have it working. This speeds up the development process since you are not waiting for the full SAM build/deploy cycle to finish for each minor change you make. If you go with this approach, be sure to copy the final code from the console and paste it into the file in this repository!

If your new Lambda is NodeJS, you will need to update the `lambdas/api/sam_build_deploy.rb` script so that it runs `npm run build` for your function. See an example by searching for `get_dmps_downloads` within the script.

**Modifying an API Lambda Function**
We recommend logging into the AWS console (dev account) and navigating to the Lambda function you want to modify. From there you can run tests and make small modifications until you have it working. This speeds up the development process since you are not waiting for the full SAM build/deploy cycle to finish for each minor change you make.

Once you are happy with the changes, copy the final code from the console and paste it into the file in this repository!

__For JS based Lambdas:__
You will first need to build the index.js file. To do that navigate to the `lambdas/api/[lambda function]` directory and run `npm run build` (note you may need to run `npm install` if this is the first time)

If your new Lambda is NodeJS, you will need to update the `lambdas/api/sam_build_deploy.rb` script so that it runs `npm run build` for your function. See an example by searching for `get_dmps_downloads` within the script.

For information on updating the NodeJS Lambda Layers, please see the README in `lambdas/layers/dmptool`

__For all Lambdas:__
You should then navigate to the `lambdas/api/` directory and run `ruby sam_build_deploy.rb [env] true true [log_level]`.

**Modifying an Indexer, Harvester or Utility Lambda Function**
We recommend logging into the AWS console (dev account) and navigating to the Lambda function you want to modify. From there you can run tests and make small modifications until you have it working. This speeds up the development process since you are not waiting for the full SAM build/deploy cycle to finish for each minor change you make.

Once you are happy with the changes, copy the final code from the console and paste it into the file in this repository!

You should then run `ruby sam_build_deploy.rb [env] true true [log_level]`.

**Updating dependencies (gem and JS)**
You will need to update the versions of gems and JS packages managed by other parties (e.g. aws) on a fairly frequent basis. To do this you should:

- Navigate to the `landing_page/` directory and run `npm upgrade` and then deploy the changes by running `ruby build_deploy.rb [env]`
- Navigate to the `gems/` directory and update the gem dependencies for each one. To do this:
  - delete the current `*.gem` file
  - run `bundle update` to update all dependencies
  - open the `lib/[gem_name]/version.rb` and increment the number
  - rebuild the gem by running `gem build [gem_name].gemspec`
  - publish the gem by running `gem push [new_gemfile_name]`
- Navigate to `lambdas/layers/` directory and then update each layer:
  - Navigate to `lambdas/layers/baseline` and run `ruby sam_build_deploy.rb [env] true true [log_level]`. This will deploy the Layer and then each individual Lambda that uses it
  - Navigate to `lambdas/layers/dmptool` and within each of the subdirectories run `./build_deploy.sh [env]`
  - Navigate to the `lambdas/layers/api` and run `ruby sam_build_deploy.rb [env] true true [log_level]`.

**Modifying the DMP Landing page**
The DMP ID landing page is a static React JS webspage that makes an API call to the API Gateway to fetch the JSON for the DMP ID. The JSON is then used to render the page.
Once deployed the landing pages can be accessed at `https://your.domain.edu/dmps/[doi]`

Navigate to the `landing_page/` directory and modify the JS/SASS code as necessary.

For development, you can run `npm start` to compile (and watch) the JS and SCSS files and view the site in your local browser. You will need to know the DMP ID of a valid DMP that currently exists in your development environment.
For example, `http://loclhost:3000/dmps/10.12345/ABCD1234` will fetch the JSON metadata for the latest version of the `10.12345/ABCD1234` DMP ID and render the landing page. If the DMP ID could not be found then React will render a 'Not Found' page.

Once you have finished you can run `ruby build_deploy.rb [env]` to deploy the changes to the S3 bucket and refresh the CloudFront Distribution's cache

**OpenAPI (aka Swagger) UI**
Your CloudFront distribution will contain Swagger API documentation for your API at `https://your.domain.edu/api-docs`.

Navigate to the `swagger` directory and modify the `v0-openapi-template.json` file as needed.

If you need to modify the DMP JSON structure, you will need to do so in JSON schema file located in the `gems/uc3-dmp-id/lib/uc3-dmp-id/schemas/author.rb` directory. The build script below pulls this schema file in and transforms it for the openapi format.

Run `ruby build_openapi_spec.rb [env] [swagger-ui-version]` to build the OpenApi files, deploy them to the S3 bucket, and refresh the CloudFront Distribution's cache

**We do NOT recommend deploying this to your production environment! It will give users the ability to create/update/tombstone DMPs.**

## Deleting Layers and Functions

To completely delete a layer of function, navigate to the appropriate directory and run `ruby sam_build_deploy.rb [env] false false`

## Testing
We have an integration test that can be used to test most of the deployed Lambda functions (everything except the harvesters).

You will need to login to the AWS console and navigate to the Cognito UserPool. Once there, go to the App Integration tab and find the client account you want to test with. Copy the Client Id and Client Secret, you will use them to run the command.

To test you system, navigate to the `test/` directory and run `ruby integration_test.rb dev [client_id] [client_secret]`
