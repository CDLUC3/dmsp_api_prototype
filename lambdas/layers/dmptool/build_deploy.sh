#!/bin/bash

if [ $# -lt 1 ]; then
  echo 'You MUST specify the environment! (e.g. dev, stg, prd)'
  exit 1
fi

echo "Building and deploying all JS layers for environment: $1"
echo "---------------------------------------------------"

cd ./cloudformation && ./build_deploy.sh $1 && cd ..

cd ./cognito && ./build_deploy.sh $1 && cd ..

cd ./database && ./build_deploy.sh $1 && cd ..

cd ./general && ./build_deploy.sh $1 && cd ..

cd ./logger && ./build_deploy.sh $1 && cd ..

cd ./s3 && ./build_deploy.sh $1 && cd ..

cd ./ssm && ./build_deploy.sh $1 && cd ..

echo "---------------------------------------------------"
echo "All JS layers built and deployed for environment: $1"
