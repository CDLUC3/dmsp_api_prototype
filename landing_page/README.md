# Building

To build and deploy the landing stage code, you must be logged into AWS and have your credentials setup locally. Then run `ruby build_deploy.rb <env>`.

This will: 
1. Create a `./build` directory with the built code. 
2. Sync that directory to the S3 bucket for the given environment.
3. Invalidate the CloudFront distribution's cache so that it immediately starts serving the new code.

# Testing

Because the build and deploy process is so fast, you can test chnages by running the build on the dev or stg environments. Then visit either the dmptool-dev.cdlib.org or dmptool-stg.cdlib.org and find a DMP with a published DOI. Then click on that DOI to be taken to the landing page.