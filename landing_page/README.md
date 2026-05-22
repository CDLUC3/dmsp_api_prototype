# DMP Tool Landing Page

## Setup

Make sure you have the following installed on your local machine:
- Ruby 3.3+
- Bundler
- AWS CLI
- Node 22+
- NPM 8+

Then run `bundle install` to install the gems.
Then run `npm install` to install the NPM packages.

Create a `src/tmp.js` file to define your App name and URL. It should look something like this:
```aiignore
export const DMPTOOL_NAME = 'My Application';
export const DMPTOOL_URL = 'https://example.com/';

```

## Building

To build and deploy the landing stage code, you must be logged into AWS and have your credentials setup locally. Then run `ruby build_deploy.rb <env>`.

This will: 
1. Create a `./build` directory with the built code. 
2. Sync that directory to the S3 bucket for the given environment.
3. Invalidate the CloudFront distribution's cache so that it immediately starts serving the new code.

## Testing

Because the build and deploy process is so fast, you can test chnages by running the build on the dev or stg environments. Then visit either the dmptool-dev.cdlib.org or dmptool-stg.cdlib.org and find a DMP with a published DOI. Then click on that DOI to be taken to the landing page.