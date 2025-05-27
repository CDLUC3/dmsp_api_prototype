#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Usage: find-dmp-narrative.sh <env> <dmp_id>"
  echo
  echo "   <env>    the 3 character environment name (e.g., dev, stg, prd)"
  echo "   <dmp_id> the DMP ID to search for (e.g. doi.org/10.12345/A1B2C3)"
  exit 1
fi

PREFIX_QUERY="Stacks[0].Outputs[?OutputKey=='S3CloudFrontBucketId'].OutputValue"
BUCKET=$(aws cloudformation describe-stacks --stack-name "uc3-dmp-hub-${1}-regional-s3" --query $PREFIX_QUERY --output text)
PREFIX=narratives/
KEY_TAG_KEY=DMP_ID

# List objects (you can add --prefix if needed)
aws s3api list-objects-v2 \
  --bucket "$BUCKET" \
  --prefix "$PREFIX" \
  --query "Contents[].Key" \
  --output json | jq -r '.[]' | while read -r key; do

    # Get tags for the object
    tags=$(aws s3api get-object-tagging --bucket "$BUCKET" --key "$key" \
      --query "TagSet[?Key=='$KEY_TAG_KEY' && Value=='$2']" \
      --output text)

    if [[ -n "$tags" ]]; then
      echo "Found match: $key"
    fi
done
